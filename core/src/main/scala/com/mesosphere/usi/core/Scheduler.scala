package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.{BidiShape, FlowShape}
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL}
import com.mesosphere.usi.core.models.AgentId
import com.mesosphere.usi.models.{Mesos, SpecEvent, USIStateEvent}

import scala.concurrent.Future

/*
 * Provides the scheduler graph component. The component has two inputs, and two outputs:
 *
 * Input:
 * 1) SpecEvents - Used to replicate the specification state from the framework to the USI scheduler
 * 2) MesosEvents - Events from Mesos; offers, task status updates, etc.
 *
 * Output:
 * 1) USIStateEvents - Used to replicate the state of pods, agents, and reservations to the framework
 * 2) MesosCalls - Actions, such as revive, kill, accept offer, etc., used to realize the specification.
 *
 * Fully wired, the graph looks like this at a high-level view:
 *
 *                                                 *** SCHEDULER ***
 *                    +------------------------------------------------------------------------+
 *                    |                                                                        |
 *                    |  +------------------+          +-------------+         USIStateEvents  |
 *        SpecEvents  |  |                  |          |             |      /------------------>----> (framework)
 * (framework) >------>-->                  |          |             |     /                   |
 *                    |  |                  | Effects  |             |    /                    |
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

  def connect(mesosHostName: String): Future[(MesosConnection, Flow[SpecEvent, USIStateEvent, NotUsed])] = {
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
  def unconnectedGraph: BidiFlow[SpecEvent, USIStateEvent, Mesos.Event, Mesos.Call, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[FrameEffects](2))

          schedulerLogic.out ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { effects =>
            effects.mesosCalls
          }
          val stateEvents = broadcast.out(1).mapConcat { effects =>
            effects.stateEvents
          }

          BidiShape.apply(schedulerLogic.in0, stateEvents.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }
}
