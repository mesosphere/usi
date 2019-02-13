package com.mesosphere.usi.core

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink}
import akka.stream.{ActorMaterializer, BidiShape, FlowShape}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.models.{SpecEvent, StateEvent}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.Protos.{FrameworkID, FrameworkInfo}

import scala.concurrent.Future

/*
 * Provides the scheduler graph component. The component has two inputs, and two outputs:
 *
 * Input:
 * 1) SpecEvents - Used to replicate the specification state from the framework implementation to the USI SchedulerLogic
 * 2) MesosEvents - Events from Mesos; offers, task status updates, etc.
 *
 * Output:
 * 1) StateEvents - Used to replicate the state of pods, agents, and reservations to the framework
 * 2) MesosCalls - Actions, such as revive, kill, accept offer, etc., used to realize the specification.
 *
 * Fully wired, the graph looks like this at a high-level view:
 *
 *                                                 *** SCHEDULER ***
 *                    +------------------------------------------------------------------------+
 *                    |                                                                        |
 *                    |  +------------------+          +-------------+         StateEvents     |
 *        SpecEvents  |  |                  |          |             |      /------------------>----> (framework)
 * (framework) >------>-->                  | Frame-   |             |     /                   |
 *                    |  |                  | Results  |             |    /                    |
 *                    |  |  SchedulerLogic  o----------> Persistence o---+                     |
 *                    |  |                  |          |             |    \                    |
 *       MesosEvents  |  |                  |          |             |     \   MesosCalls      |
 *       /------------>-->                  |          |             |      \------------------>
 *      /             |  |                  |          |             |                         |\
 *     /              |  +------------------+          +-------------+                         | \
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
  case class MesosConnection(mesosHostName: String, mesosCallFactory: MesosCalls, frameworkId: FrameworkID, schedulerFlow: Flow[SpecEvent, StateEvent, NotUsed])

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

  def mesosConnectedGraph(mesosCallFactory: MesosCalls, mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SpecEvent, StateEvent, NotUsed] = {
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

  /*
   */
  def unconnectedGraph(mesosCallFactory: MesosCalls): BidiFlow[SpecEvent, StateEvent, MesosEvent, MesosCall, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph(mesosCallFactory)) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[FrameResult](2))

          schedulerLogic.out ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { frameResult =>
            frameResult.mesosIntents
          }
          val stateEvents = broadcast.out(1).mapConcat { frameResult =>
            frameResult.stateEvents
          }

          BidiShape.apply(schedulerLogic.in0, stateEvents.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }
}
