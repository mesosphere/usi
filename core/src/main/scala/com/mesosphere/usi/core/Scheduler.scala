package com.mesosphere.usi.core

import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{BidiShape, FanOutShape2, Graph, Materializer}
import akka.{Done, NotUsed}
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{PodRecordUpdatedEvent, PodSpecUpdatedEvent, StateEvent, StateSnapshot}
import com.mesosphere.usi.repository.PodRecordRepository
import com.typesafe.scalalogging.StrictLogging
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
  * At a high level, the graph looks like this:
  * {{{
  *
  *                                                 *** SCHEDULER ***
  *                    +-------------------------------------------------------------------------------------------+
  *                    |                                                                                           |
  *                    |  +------------------+                                                    USI State Events |
  *  SchedulerCommands |  |                  |                                                  /------------------>----> (framework)
  * (framework) >------>-->                  | Scheduler                  +-------------+      /                   |
  *                    |  |                  |  Events                    |             |     /                    |
  *                    |  |  SchedulerLogic  o-------o--------------------> Persistence o----+                     |
  *                    |  |                  |        \                   |             |     \                    |
  *       Mesos Events |  |                  |         \                  +-------------+      \   Mesos Calls     |
  *       /------------>-->                  |          \           +---------------------+     +------------------>
  *      /             |  |                  |           \          |                     |    /                   |\
  *     /              |  +------------------+            ---------->  Suppress / Revive  o---/                    | \
  *    /               |                            PodSpecUpdated  |                     |                        |  \
  *   |                |                               Events       +---------------------+                        |  \
  *   |                |                                                                                           |  \
  *   |                +-------------------------------------------------------------------------------------------+  |
  *    \                                                                                                              |
  *     \                                         +----------------------+                                           /
  *      \                                        |                      |                                          /
  *       \---------------------------------------<        Mesos         <------------------------------------------
  *                                               |                      |
  *                                               +----------------------+
  * }}}
  */
object Scheduler extends StrictLogging {
  type Factory = SchedulerLogicFactory with PersistenceFlowFactory with SuppressReviveFactory with MesosFlowFactory

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @return Snapshot of the current state, as well as Source which produces [[StateEvent]]s and Sink which accepts [[SchedulerCommand]]s.
    */
  def asSourceAndSink(factory: Factory)(implicit mat: Materializer)
    : Future[(StateSnapshot, Source[StateEvent, NotUsed], Sink[SchedulerCommand, Future[Done]])] = {
    asFlow(factory).map {
      case (snapshot, schedulerFlow) =>
        val (source, sink) = FlowHelpers.asSourceAndSink(schedulerFlow)(mat)
        (snapshot, source, sink)
    }(CallerThreadExecutionContext.context)
  }

  /**
    * Represents the scheduler as a Flow.
    * @param factory
    * @return Snapshot of the current state, as well as Flow that produces [[StateEvent]]s and accepts [[SchedulerCommand]]s.
    */
  def asFlow(factory: Factory): Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] = {
    if (!isMultiRoleFramework(factory.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    factory
      .loadSnapshot()
      .map { snapshot =>
        val graph = schedulerGraph(snapshot, factory)
        snapshot -> graph.join(factory.newMesosFlow)
      }(CallerThreadExecutionContext.context)
  }

  private[usi] def schedulerGraph(
      snapshot: StateSnapshot,
      factory: Factory): BidiFlow[SchedulerCommand, MesosCall, MesosEvent, StateEvent, NotUsed] = {
    val schedulerLogicGraph = factory.newSchedulerLogicGraph(snapshot)
    val suppressReviveFlow = factory.newSuppressReviveFlow
    BidiFlow.fromGraph {
      GraphDSL.create(schedulerLogicGraph, suppressReviveFlow)((_, _) => NotUsed) {
        implicit builder => (schedulerLogic, suppressRevive) =>
          {
            import GraphDSL.Implicits._

            // We disable eagerComplete for the fanin because:
            // * It can cause a circular cancellation / failure race.
            // * In the case of upstream cancellation, we want to finish processing any pending elements for the fan-in
            // * A failure on either side will cause the merge to fail, anyways
            val schedulerEventsBroadcast =
              builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true).named("schedulerEventsBroadcast"))
            val mesosCallsFanIn = builder.add(Merge[MesosCall](2, eagerComplete = false).named("mergeMesosCalls"))
            val (schedulerEventsRouterIn, stateEvents, mesosCalls) = {
              val g = builder.add(routeEvents)
              (g.in, g.out0, g.out1)

            }
            val collectPodSpecEvents = builder.add(
              Flow[SchedulerEvents]
                .mapConcat(_.stateEvents)
                .collect { case psu: PodSpecUpdatedEvent => psu }
                .named("collectPodSpecEvents"))
            val persistenceStorageFlow = builder.add(factory.newPersistenceFlow.named("persistenceFlow"))

            schedulerLogic.out ~> schedulerEventsBroadcast
            schedulerEventsBroadcast ~> collectPodSpecEvents ~> suppressRevive ~> mesosCallsFanIn
            schedulerEventsBroadcast ~> persistenceStorageFlow ~> schedulerEventsRouterIn

            mesosCalls ~> mesosCallsFanIn

            BidiShape.apply(schedulerLogic.in0, mesosCallsFanIn.out, schedulerLogic.in1, stateEvents)
          }
      }
    }
  }

  private def routeEvents: Graph[FanOutShape2[SchedulerEvents, StateEvent, MesosCall], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val b = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true).named("routeSchedulerEvents"))
      val getStateEvents = builder.add(
        Flow[SchedulerEvents].mapConcat(_.stateEvents).named("stateEvents").log("routeEvents - state events"))
      val getMesosCalls =
        builder.add(Flow[SchedulerEvents].mapConcat(_.mesosCalls).named("mesosCalls").log("routeEvents - mesos calls"))
      b ~> getStateEvents
      b ~> getMesosCalls

      new FanOutShape2(b.in, getStateEvents.out, getMesosCalls.out)
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
