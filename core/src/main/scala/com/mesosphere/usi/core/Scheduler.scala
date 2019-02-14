package com.mesosphere.usi.core

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink, Source}
import akka.stream.{ActorMaterializer, BidiShape, FlowShape}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.models._
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.Protos.{FrameworkID, FrameworkInfo}

import scala.concurrent.Future

/*
 * Provides the scheduler graph component. The component has two inputs, and two outputs:
 *
 * Input:
 * 1) SpecInput - Used to replicate the specification state from the framework implementation to the USI SchedulerLogic;
 *    First, a spec snapshot, followed by spec updates.
 * 2) MesosEvents - Events from Mesos; offers, task status updates, etc.
 *
 * Output:
 * 1) StateEvents - Used to replicate the state of pods, agents, and reservations to the framework
 *    First, a scheduler state snapshot, followed by state updates.
 * 2) MesosCalls - Actions, such as revive, kill, accept offer, etc., used to realize the specification.
 *
 * Fully wired, the graph looks like this at a high-level view:
 *
 *                                                 *** SCHEDULER ***
 *                    +------------------------------------------------------------------------+
 *                    |                                                                        |
 *                    |  +------------------+           +-------------+        StateOutput     |
 *        SpecInput   |  |                  |           |             |     /------------------>----> (framework)
 * (framework) >------>-->                  | Scheduler |             |    /                   |
 *                    |  |                  |  Events   |             |   /                    |
 *                    |  |  SchedulerLogic  o-----------> Persistence o--+                     |
 *                    |  |                  |           |             |   \                    |
 *       MesosEvents  |  |                  |           |             |    \   MesosCalls      |
 *       /------------>-->                  |           |             |     \------------------>
 *      /             |  |                  |           |             |                        |\
 *     /              |  +------------------+           +-------------+                        | \
 *    /               |                                                                        |  \
 *   |                +------------------------------------------------------------------------+  |
 *    \                                                                                           |
 *     \                               +----------------------+                                  /
 *      \                              |                      |                                 /
 *       \-----------------------------<        Mesos         <---------------------------------
 *                                     |                      |
 *                                     +----------------------+
 */
object Scheduler {
  type SpecInput = (SpecsSnapshot, Source[SpecUpdated, Any])

  type StateOutput = (StateSnapshot, Source[StateEvent, Any])

  case class MesosConnection(mesosHostName: String, mesosCallFactory: MesosCalls, frameworkId: FrameworkID, schedulerFlow: Flow[SpecInput, StateOutput, NotUsed])

  // TODO - provide abstraction for FrameworkID persistence, recovery, etc.; we'll want to exclude the frameworkId
  // TODO - This layer should provide a cancellation mechanism and an auto-reconnect mechanism
  def connect(settings: MesosClientSettings, frameworkInfo: FrameworkInfo)(
      implicit actorSystem: ActorSystem,
      materializer: ActorMaterializer): Future[MesosConnection] = {

    import scala.concurrent.ExecutionContext.Implicits.global

    MesosClient(settings, frameworkInfo).runWith(Sink.head).map { client =>
      val flow = mesosConnectedGraph(client.calls, Flow.fromSinkAndSource(client.mesosSink, client.mesosSource))
      MesosConnection(settings.master, client.calls, client.frameworkId, flow)
    }
  }

  def mesosConnectedGraph(mesosCallFactory: MesosCalls, mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SpecInput, StateOutput, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(unconnectedGraph(mesosCallFactory), mesosFlow)((_, _) => NotUsed) { implicit builder =>
        { (graph, mesos) =>
          import GraphDSL.Implicits._

          mesos ~> graph.in2
          graph.out2 ~> mesos

          FlowShape(graph.in1, graph.out1)
        }
      }
    }
  }

  /**
    * We expess an interface of receiving a snapshot, followed by a series of events, to guide consumers of USI down a
    * proper implementation path. A SpecSnapshot should be the very first thing that the USI Scheduler receives from the
    * Framework implementation.
    *
    * However, for convenience in working with streams, internally we deal with a single stream of SpecEvents.
    */
  private val specInputFlatteningFlow: Flow[SpecInput, SpecEvent, NotUsed] = Flow[SpecInput].flatMapConcat { case (snapshot, rest) =>
    rest.prepend(Source.single(snapshot))
  }

  private val stateOutputBreakoutFlow: Flow[StateEvent, StateOutput, NotUsed] = Flow[StateEvent].prefixAndTail(0).map { case (_, stateEvents) =>
    // TODO (DCOS-47476) use actual prefixAndTail and expect first event to be a Snapshot
    val stateUpdates = stateEvents.map {
      case c: StateUpdated => c
      case _ => ???
    }
    (StateSnapshot.empty, stateUpdates)
  }

  def unconnectedGraph(mesosCallFactory: MesosCalls): BidiFlow[SpecInput, StateOutput, MesosEvent, MesosCall, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph(mesosCallFactory)) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[SchedulerEvents](2))
          val specInputFlattening = builder.add(specInputFlatteningFlow)
          val stateOutputBreakout = builder.add(stateOutputBreakoutFlow)

          specInputFlattening ~> schedulerLogic.in0
          schedulerLogic.out ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { frameResult =>
            frameResult.mesosCalls
          }
          val stateEvents = broadcast.out(1).mapConcat { frameResult =>
            frameResult.stateEvents
          }

          stateEvents ~> stateOutputBreakout

          BidiShape.apply(specInputFlattening.in, stateOutputBreakout.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }
}
