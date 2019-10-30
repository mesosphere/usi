package com.mesosphere.usi.core

import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink, Source}
import akka.stream.{BidiShape, FlowShape, Materializer}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{PodRecordUpdatedEvent, StateEvent, StateSnapshot}
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides the scheduler graph component. The component has two inputs, and two outputs:
  *
  * Input:
  * 1) SchedulerCommands - Receives SchedulerCommand objects to launch, kill/destroy, and remove pods or reservations.
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
  *  SchedulerCommands |  |                  |           |             |     /------------------>----> (framework)
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

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      metrics: Metrics,
      schedulerSettings: SchedulerSettings)(implicit mat: Materializer)
    : Future[(StateSnapshot, Source[StateEvent, NotUsed], Sink[SchedulerCommand, Future[Done]])] = {
    fromClient(client, podRecordRepository, metrics, schedulerSettings).map {
      case (snapshot, flow) =>
        val (source, sink) = FlowHelpers.asSourceAndSink(flow)(mat)
        (snapshot, source, sink)
    }(CallerThreadExecutionContext.context)
  }

  private[usi] def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      metrics: Metrics,
      schedulerSettings: SchedulerSettings): Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    fromFlow(
      client.calls,
      podRecordRepository,
      metrics,
      Flow.fromSinkAndSourceCoupled(client.mesosSink, client.mesosSource),
      schedulerSettings,
      client.masterInfo.getDomain
    )
  }

  private[usi] def fromFlow(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      metrics: Metrics,
      mesosFlow: Flow[MesosCall, MesosEvent, Any],
      schedulerSettings: SchedulerSettings,
      masterDomainInfo: Protos.DomainInfo): Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] = {
    unconnectedGraph(mesosCallFactory, podRecordRepository, metrics, schedulerSettings, masterDomainInfo).map {
      case (snapshot, graph) =>
        val flow = Flow.fromGraph {
          GraphDSL.create(graph, mesosFlow)((_, _) => NotUsed) { implicit builder =>
            { (graph, mesos) =>
              import GraphDSL.Implicits._

              mesos ~> graph.in2
              graph.out2 ~> mesos

              FlowShape(graph.in1, graph.out1)
            }
          }
        }
        snapshot -> flow
    }(CallerThreadExecutionContext.context)
  }

  private[core] def unconnectedGraph(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      metrics: Metrics,
      schedulerSettings: SchedulerSettings,
      masterDomainInfo: Protos.DomainInfo)
    : Future[(StateSnapshot, BidiFlow[SchedulerCommand, StateEvent, MesosEvent, MesosCall, NotUsed])] = {
    podRecordRepository
      .readAll()
      .map { podRecords =>
        val snapshot = StateSnapshot(podRecords = podRecords.values.toSeq, agentRecords = Nil)
        val schedulerLogicGraph = new SchedulerLogicGraph(mesosCallFactory, masterDomainInfo, snapshot, metrics)
        val bidiFlow = BidiFlow.fromGraph {
          GraphDSL.create(schedulerLogicGraph) { implicit builder => (schedulerLogic) =>
            {
              import GraphDSL.Implicits._

              val broadcast = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
              val persistenceStorageFlow = builder.add(persistenceFlow(podRecordRepository, schedulerSettings))

              schedulerLogic.out ~> persistenceStorageFlow ~> broadcast.in

              val mesosCalls = broadcast.out(0).mapConcat { frameResult =>
                frameResult.mesosCalls
              }
              val stateEvents = broadcast.out(1).mapConcat { frameResult =>
                frameResult.stateEvents
              }

              BidiShape.apply(schedulerLogic.in0, stateEvents.outlet, schedulerLogic.in1, mesosCalls.outlet)
            }
          }
        }
        snapshot -> bidiFlow

      }(ExecutionContext.global)
  }

  private[core] def persistenceFlow(
      podRecordRepository: PodRecordRepository,
      schedulerSettings: SchedulerSettings): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Flow[SchedulerEvents]
      .mapConcat(persistEvents(_, podRecordRepository))
      .mapAsync(schedulerSettings.persistencePipelineLimit)(call => call())
      .collect { case Some(events) => events }
  }

  private def persistEvents(
      events: SchedulerEvents,
      podRecordRepository: PodRecordRepository): List[() => Future[Option[SchedulerEvents]]] = {
    val ops: List[() => Future[Option[SchedulerEvents]]] = events.stateEvents.collect {
      case PodRecordUpdatedEvent(_, Some(podRecord)) =>
        () =>
          podRecordRepository.store(podRecord).map(_ => None)(CallerThreadExecutionContext.context)
      case PodRecordUpdatedEvent(podId, None) =>
        () =>
          podRecordRepository.delete(podId).map(_ => None)(CallerThreadExecutionContext.context)
    }
    ops :+ (() => Future.successful(Some(events)))
  }

  private def isMultiRoleFramework(frameworkInfo: FrameworkInfo): Boolean =
    frameworkInfo.getCapabilitiesList.asScala.exists(_.getType == FrameworkInfo.Capability.Type.MULTI_ROLE)
}
