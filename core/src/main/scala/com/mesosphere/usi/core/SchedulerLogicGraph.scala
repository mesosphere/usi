package com.mesosphere.usi.core

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}
import com.mesosphere.usi.models.{Mesos, SpecEvent}

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
class SchedulerLogicGraph extends GraphStage[FanInShape2[SpecEvent, Mesos.Event, FrameEffects]] {
  import SchedulerLogicGraph.BUFFER_SIZE

  val mesosEvents = Inlet[Mesos.Event]("mesos-events")
  val specEvents = Inlet[SpecEvent]("specs")
  val responses = Outlet[FrameEffects]("response")
  // Define the (sole) output port of this stage
  val out: Outlet[Int] = Outlet("NumbersSource")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: FanInShape2[SpecEvent, Mesos.Event, FrameEffects] = new FanInShape2(specEvents, mesosEvents, responses)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val schedulerLogic = new SchedulerLogic

    new GraphStageLogic(shape) {
      val pendingResponses: mutable.Queue[FrameEffects] = mutable.Queue.empty

      setHandler(mesosEvents, new InHandler {
        override def onPush(): Unit = {
          handleResponse(schedulerLogic.processMesosEvent(grab(mesosEvents)))
          maybePull()
        }
      })

      setHandler(specEvents, new InHandler {
        override def onPush(): Unit = {
          handleResponse(schedulerLogic.processSpecEvent(grab(specEvents)))
          maybePull()
        }
      })

      setHandler(responses, new OutHandler {
        override def onPull(): Unit = {
          if (pendingResponses.nonEmpty) {
            push(responses, pendingResponses.dequeue())
            maybePull()
          }
        }
      })

      override def preStart(): Unit = {
        // Start the stream
        pull(specEvents)
        pull(mesosEvents)
      }

      def handleResponse(response: FrameEffects): Unit = {
        if (isAvailable(responses)) {
          if (pendingResponses.nonEmpty) {
            throw new IllegalStateException("We should always immediately push on pull if responses are queued")
          }
          push(responses, response)
        } else {
          pendingResponses.enqueue(response)
        }
        maybePull()
      }

      def maybePull(): Unit = {
        if (pendingResponses.length < BUFFER_SIZE) {
          if (!hasBeenPulled(mesosEvents))
            pull(mesosEvents)
          if (!hasBeenPulled(specEvents))
            pull(specEvents)
        }
      }
    }
  }
}
