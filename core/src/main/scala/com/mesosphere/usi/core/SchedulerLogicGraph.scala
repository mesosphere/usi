package com.mesosphere.usi.core

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.models.{PodId, PodRecord, SpecEvent}
import org.apache.mesos.v1.scheduler.Protos.{Event => MesosEvent}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object SchedulerLogicGraph {
  val BUFFER_SIZE = 32
}

/**
  * A simple stateful fan-in graph stage that instantiates an instance of the SchedulerLogic, feeding messages from each
  * of the two inputs as appropriate.
  *
  * This graph component looks like this:
  *
  *      Spec Events   +---------------------+
  *   ----------------->                     |
  *                    |                     |   Response
  *                    |   Scheduler Logic   >-------------
  *     Mesos Events   |                     |
  *   ----------------->                     |
  *                    +---------------------+
  *
  * Only one event is processed at a time.
  *
  * For more detail, see Scheduler
  *
  * The component has an internal buffer; if this buffer is above BUFFER_SIZE, then it will stop pulling.
  *
  * Author's Note:
  *
  * This was prematurely implemented; a simple fan-in merge component would suffice for now. In early
  * development phases of the scheduler, it seemed that having an actual BIDI flow stage with robust stream termination
  * logic would've been needed.
  *
  * It's existence is only warranted by forecasted future needs. It's kept as a graph with an internal buffer as we will
  * likely need timers, other callbacks, and additional output ports (such as an offer event stream?).
  */
private[core] class SchedulerLogicGraph(
    mesosCallFactory: MesosCalls,
    initialPodRecords: => Future[Map[PodId, PodRecord]])
    extends GraphStage[FanInShape2[SpecEvent, MesosEvent, SchedulerEvents]] {
  import SchedulerLogicGraph.BUFFER_SIZE

  private val mesosEventsInlet = Inlet[MesosEvent]("mesos-events")
  private val specEventsInlet = Inlet[SpecEvent]("specs")
  private val frameResultOutlet = Outlet[SchedulerEvents]("effects")

  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: FanInShape2[SpecEvent, MesosEvent, SchedulerEvents] =
    new FanInShape2(specEventsInlet, mesosEventsInlet, frameResultOutlet)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val handler = new SchedulerLogicHandler(mesosCallFactory)

    new GraphStageLogic(shape) {
      val pendingEffects: mutable.Queue[SchedulerEvents] = mutable.Queue.empty

      setHandler(mesosEventsInlet, new InHandler {
        override def onPush(): Unit = {
          pushOrQueueIntents(handler.handleMesosEvent(grab(mesosEventsInlet)))
          maybePull()
        }
      })

      setHandler(specEventsInlet, new InHandler {
        override def onPush(): Unit = {
          pushOrQueueIntents(handler.handleSpecEvent(grab(specEventsInlet)))
          maybePull()
        }
      })

      setHandler(frameResultOutlet, new OutHandler {
        override def onPull(): Unit = {
          if (pendingEffects.nonEmpty) {
            push(frameResultOutlet, pendingEffects.dequeue())
            maybePull()
          }
        }
      })

      val startGraph = this.getAsyncCallback[Try[Map[PodId, PodRecord]]] {
        case Success(initialSnapshot) =>
          handler.loadInitialPodRecords(initialSnapshot)
          maybePull()
        case Failure(ex) =>
          this.failStage(ex)
      }

      override def preStart(): Unit =
        initialPodRecords.onComplete(startGraph.invoke)(CallerThreadExecutionContext.context)

      def pushOrQueueIntents(effects: SchedulerEvents): Unit = {
        if (isAvailable(frameResultOutlet)) {
          if (pendingEffects.nonEmpty) {
            throw new IllegalStateException("We should always immediately push on pull if effects are queued")
          }
          push(frameResultOutlet, effects)
        } else {
          pendingEffects.enqueue(effects)
        }
        maybePull()
      }

      def maybePull(): Unit = {
        if (pendingEffects.length < BUFFER_SIZE) {
          if (!hasBeenPulled(mesosEventsInlet))
            pull(mesosEventsInlet)
          if (!hasBeenPulled(specEventsInlet))
            pull(specEventsInlet)
        }
      }
    }
  }
}
