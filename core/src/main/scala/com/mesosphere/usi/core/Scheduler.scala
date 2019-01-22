package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Broadcast, GraphDSL}
import com.mesosphere.usi.models.{Mesos, SpecEvent, USIStateEvent}


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
 *                    |  |                  | Response |             |    /                    |
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
   /*
    */
  def graph: BidiFlow[SpecEvent, USIStateEvent, Mesos.Event, Mesos.Call, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph) { implicit builder =>
        (schedulerLogic) => {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[FrameResult](2))

          schedulerLogic.out ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { response => response.mesosCalls }
          val statuses = broadcast.out(1).mapConcat { response => response.usiStateEvents }

          BidiShape.apply(schedulerLogic.in0, statuses.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }
}
