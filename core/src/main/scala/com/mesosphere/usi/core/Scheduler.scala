package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL}
import akka.stream.{BidiShape, FlowShape}
import com.mesosphere.usi.core.models.{Mesos, SpecEvent, StateEvent}

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
  case class MesosConnection(mesosHostName: String)

  def connect(mesosHostName: String): Future[(MesosConnection, Flow[SpecEvent, StateEvent, NotUsed])] = {
    val flow = Flow.fromGraph {
      GraphDSL.create(Scheduler.unconnectedGraph, FakeMesos.flow)((_, _) => NotUsed) { implicit builder =>
        { (graph, mockMesos) =>
          import GraphDSL.Implicits._

          mockMesos ~> graph.in2
          graph.out2 ~> mockMesos

          FlowShape(graph.in1, graph.out1)
        }
      }
    }
    Future.successful(MesosConnection(mesosHostName) -> flow)
  }

  /*
   */
  def unconnectedGraph: BidiFlow[SpecEvent, StateEvent, Mesos.Event, Mesos.Call, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph) { implicit builder => (schedulerLogic) =>
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
