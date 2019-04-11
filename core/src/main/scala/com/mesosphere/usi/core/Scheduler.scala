package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Source}
import akka.stream.{BidiShape, FlowShape}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.models.{SpecEvent, SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._

/**
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
  * {{{
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
  * }}}
  */
object Scheduler {
  type SpecInput = (SpecsSnapshot, Source[SpecUpdated, Any])

  type StateOutput = (StateSnapshot, Source[StateEvent, Any])

  def fromSnapshot(specsSnapshot: SpecsSnapshot, client: MesosClient): Flow[SpecUpdated, StateOutput, NotUsed] =
    Flow[SpecUpdated].prefixAndTail(0).map { case (_, rest) => specsSnapshot -> rest }.via(fromClient(client))

  def fromClient(client: MesosClient): Flow[SpecInput, StateOutput, NotUsed] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. Please provide create MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    fromFlow(client.calls, Flow.fromSinkAndSource(client.mesosSink, client.mesosSource))
  }

  private def isMultiRoleFramework(frameworkInfo: FrameworkInfo): Boolean =
    frameworkInfo.getCapabilitiesList.asScala.exists(_.getType == FrameworkInfo.Capability.Type.MULTI_ROLE)

  def fromFlow(
      mesosCallFactory: MesosCalls,
      mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SpecInput, StateOutput, NotUsed] = {
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
    * We express an interface of receiving a snapshot, followed by a series of events, to guide consumers of USI down a
    * proper implementation path. A SpecSnapshot should be the very first thing that the USI Scheduler receives from the
    * Framework implementation.
    *
    * However, for convenience in working with streams, internally we deal with a single stream of SpecEvents.
    */
  private val specInputFlatteningFlow: Flow[SpecInput, SpecEvent, NotUsed] = Flow[SpecInput].flatMapConcat {
    case (snapshot, rest) =>
      rest.prepend(Source.single(snapshot))
  }

  // TODO (DCOS-47476) use actual prefixAndTail and expect first event to be a Snapshot; change the prefixAndTail param from 0 value to 1, let fail, etc.
  private val stateOutputBreakoutFlow: Flow[StateEvent, StateOutput, NotUsed] = Flow[StateEvent].prefixAndTail(0).map {
    case (_, stateEvents) =>
      val stateUpdates = stateEvents.map {
        case _: StateSnapshot =>
          throw new IllegalStateException("Only the first event is allowed to be a state snapshot")
        case event => event
      }
      (StateSnapshot.empty, stateUpdates)
  }

  def unconnectedGraph(
      mesosCallFactory: MesosCalls): BidiFlow[SpecInput, StateOutput, MesosEvent, MesosCall, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(new SchedulerLogicGraph(mesosCallFactory)) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
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
