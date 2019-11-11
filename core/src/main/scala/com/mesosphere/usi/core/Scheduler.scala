package com.mesosphere.usi.core

import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{BidiShape, FanOutShape2, Graph, Materializer}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{PodRecordUpdatedEvent, PodSpecUpdatedEvent, StateEvent, StateSnapshot}
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._
import scala.concurrent.Future

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
  type Factory = SchedulerLogicFactory with PersistenceFlowFactory with SuppressReviveFactory

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(factory: Factory, client: MesosClient, podRecordRepository: PodRecordRepository)(
      implicit mat: Materializer)
    : Future[(StateSnapshot, Source[StateEvent, NotUsed], Sink[SchedulerCommand, Future[Done]])] = {
    fromClient(factory, client, podRecordRepository).map {
      case (snapshot, schedulerFlow) =>
        val (source, sink) = FlowHelpers.asSourceAndSink(schedulerFlow)(mat)
        (snapshot, source, sink)
    }(CallerThreadExecutionContext.context)
  }

  private[usi] def fromClient(factory: Factory, client: MesosClient, podRecordRepository: PodRecordRepository)
    : Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    podRecordRepository
      .readAll()
      .map { podRecords =>
        val snapshot = StateSnapshot(podRecords = podRecords.values.toSeq, agentRecords = Nil)
        val graph = schedulerGraph(snapshot, factory)
        val mesosFlow = Flow.fromSinkAndSourceCoupled(client.mesosSink, client.mesosSource)
        snapshot -> graph.join(mesosFlow)
      }(CallerThreadExecutionContext.context)

  }

  private[core] def schedulerGraph(
      snapshot: StateSnapshot,
      factory: Factory): BidiFlow[SchedulerCommand, MesosCall, MesosEvent, StateEvent, NotUsed] = {
    val schedulerLogicGraph = factory.newSchedulerLogicGraph(snapshot)
    val suppressReviveFlow = factory.newSuppressReviveHandler.flow
    BidiFlow.fromGraph {
      GraphDSL.create(schedulerLogicGraph, suppressReviveFlow)((_, _) => NotUsed) {
        implicit builder => (schedulerLogic, suppressRevive) =>
          {
            import GraphDSL.Implicits._

            val (schedulerEventsInput, allStateEventsOut, podSpecEventsOut, reviveMesosCallsIn, allMesosCallsOut) = {
              val stateEventsBroadcast = builder.add(Broadcast[StateEvent](2, eagerCancel = true))
              val mesosCallsFanIn = builder.add(Merge[MesosCall](2, eagerComplete = true))
              val eventSplitter = builder.add(splitEvents)

              eventSplitter.out0 ~> stateEventsBroadcast
              eventSplitter.out1 ~> mesosCallsFanIn.in(0)

              val allStateEventsOut = stateEventsBroadcast.out(1)
              val podSpecEventsOut = stateEventsBroadcast.out(0).collect { case psu: PodSpecUpdatedEvent => psu }
              val reviveMesosCallsIn = mesosCallsFanIn.in(1)

              (eventSplitter.in, allStateEventsOut, podSpecEventsOut, reviveMesosCallsIn, mesosCallsFanIn.out)
            }

            val persistenceStorageFlow = builder.add(factory.newPersistenceFlow)

            schedulerLogic.out ~> persistenceStorageFlow ~> schedulerEventsInput

            podSpecEventsOut ~> suppressRevive ~> reviveMesosCallsIn

            BidiShape.apply(schedulerLogic.in0, allMesosCallsOut, schedulerLogic.in1, allStateEventsOut)
          }
      }
    }
  }

  private def splitEvents: Graph[FanOutShape2[SchedulerEvents, StateEvent, MesosCall], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val b = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
      val events = b.out(0).mapConcat(_.stateEvents).outlet
      val mesosCalls = b.out(1).mapConcat(_.mesosCalls).outlet

      new FanOutShape2(b.in, events, mesosCalls)
    }
  }

  private[core] def newPersistenceFlow(
      podRecordRepository: PodRecordRepository,
      persistencePipelineLimit: Int): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Flow[SchedulerEvents]
      .mapConcat(persistEvents(_, podRecordRepository))
      .mapAsync(persistencePipelineLimit)(call => call())
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
