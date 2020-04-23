package com.mesosphere.usi.core

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.models.StateSnapshot
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.metrics.Metrics
import org.apache.mesos.v1.Protos.DomainInfo
import org.apache.mesos.v1.scheduler.Protos.{Event => MesosEvent}

import scala.collection.mutable

object SchedulerLogicGraph {
  val BUFFER_SIZE = 32
}

/**
  * A simple stateful fan-in graph stage that instantiates an instance of the SchedulerLogic, feeding messages from each
  * of the two inputs as appropriate.
  *
  * This graph component looks like this:
  *
  * Scheduler Commands +---------------------+
  * ------------------->                     |
  *                    |                     |   Response
  *                    |   Scheduler Logic   >-------------
  *    Mesos Events    |                     |
  * ------------------->                     |
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
    masterDomainInfo: DomainInfo,
    initialState: StateSnapshot,
    metrics: Metrics)
    extends GraphStage[FanInShape2[SchedulerCommand, MesosEvent, SchedulerEvents]] {
  import SchedulerLogicGraph.BUFFER_SIZE

  override def initialAttributes: Attributes = super.initialAttributes.and(Attributes.name("SchedulerLogicGraph"))

  private val mesosEventsInlet = Inlet[MesosEvent]("mesos-events")
  private val schedulerCommandsInlet = Inlet[SchedulerCommand]("commands")
  private val frameResultOutlet = Outlet[SchedulerEvents]("effects")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: FanInShape2[SchedulerCommand, MesosEvent, SchedulerEvents] =
    new FanInShape2(schedulerCommandsInlet, mesosEventsInlet, frameResultOutlet)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) with StageLogging {
      private[this] val handler: SchedulerLogicHandler =
        new SchedulerLogicHandler(mesosCallFactory, masterDomainInfo, initialState, metrics)

      val pendingEffects: mutable.Queue[SchedulerEvents] = mutable.Queue.empty

      setHandler(mesosEventsInlet, new InHandler {
        override def onPush(): Unit = {
          pushOrQueueIntents(handler.handleMesosEvent(grab(mesosEventsInlet)))
          maybePull()
        }

        override def onUpstreamFinish() = {
          log.info("Mesos events finished")
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          log.error("Mesos events failed", ex)
          super.onUpstreamFailure(ex)
        }
      })

      setHandler(schedulerCommandsInlet, new InHandler {
        override def onPush(): Unit = {
          pushOrQueueIntents(handler.handleCommand(grab(schedulerCommandsInlet)))
          maybePull()
        }

        override def onUpstreamFinish() = {
          log.info("Scheduler commands finished")
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          log.error("Scheduler commands failed", ex)
          super.onUpstreamFailure(ex)
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

      override def preStart(): Unit = {
        // start the graph off
        maybePull()
      }

      def pushOrQueueIntents(effects: SchedulerEvents): Unit = {
        if (isAvailable(frameResultOutlet)) {
          if (pendingEffects.nonEmpty) {
            throw new IllegalStateException("We should always immediately push on pull if effects are queued")
          }
          log.info(s"Push pending effects $effects")
          push(frameResultOutlet, effects)
        } else {
          log.info(s"Queue pending effects $effects")
          pendingEffects.enqueue(effects)
        }
        maybePull()
      }

      def maybePull(): Unit = {
        if (pendingEffects.length < BUFFER_SIZE) {
          if (!hasBeenPulled(mesosEventsInlet))
            pull(mesosEventsInlet)
          if (!hasBeenPulled(schedulerCommandsInlet))
            pull(schedulerCommandsInlet)
        }
      }
    }
  }
}
