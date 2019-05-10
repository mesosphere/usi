package com.mesosphere.usi.core

import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.{BidiShape, FlowShape, Materializer}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{
  PodId,
  PodRecord,
  PodRecordUpdatedEvent,
  SchedulerCommand,
  StateEvent,
  StateEventOrSnapshot,
  StateSnapshot
}
import com.mesosphere.usi.repository.PodRecordRepository
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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
  type StateOutput = (StateSnapshot, Source[StateEvent, Any])

  private val schedulerSettings = SchedulerSettings.fromConfig(ConfigFactory.load().getConfig("scheduler"))

  def asFlow(client: MesosClient, podRecordRepository: PodRecordRepository)(implicit materializer: Materializer)
    : Future[(StateSnapshot, Flow[SchedulerCommand, StateEventOrSnapshot, NotUsed])] = {

    implicit val ec = ExecutionContext.global //only for ultra-fast non-blocking map

    val (snap, source, sink) = asSourceAndSink(client, podRecordRepository)

    snap.map { snapshot =>
      (snapshot, Flow.fromSinkAndSourceCoupled(sink, source))
    }
  }

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(client: MesosClient, podRecordRepository: PodRecordRepository)(implicit mat: Materializer)
    : (Future[StateSnapshot], Source[StateEventOrSnapshot, NotUsed], Sink[SchedulerCommand, Future[Done]]) = {
    val flow = fromClient(client, podRecordRepository)
    asSourceAndSink(flow)(mat)
  }

  def asSourceAndSink(schedulerFlow: Flow[SchedulerCommand, StateOutput, NotUsed])(implicit mat: Materializer)
    : (Future[StateSnapshot], Source[StateEventOrSnapshot, NotUsed], Sink[SchedulerCommand, Future[Done]]) = {

    val ((commandInputSubscriber, subscriberCompleted), commandInputSource) =
      Source.asSubscriber[SchedulerCommand].watchTermination()(Keep.both).preMaterialize()

    val firstStateOutput = commandInputSource
      .via(schedulerFlow)
      .runWith(Sink.head)

    val stateSnapshot = firstStateOutput.map { case (snapshot, _) => snapshot }(ExecutionContext.global)

    val stateEvents = Source.fromFuture(firstStateOutput).flatMapConcat { case (_, events) => events }

    (
      stateSnapshot,
      stateEvents,
      Sink.fromSubscriber(commandInputSubscriber).mapMaterializedValue(_ => subscriberCompleted))
  }

  private[usi] def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository): Flow[SchedulerCommand, StateOutput, NotUsed] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    fromFlow(client.calls, podRecordRepository, Flow.fromSinkAndSource(client.mesosSink, client.mesosSource))
  }

  private[usi] def fromFlow(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SchedulerCommand, StateOutput, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(unconnectedGraph(mesosCallFactory, podRecordRepository), mesosFlow)((_, _) => NotUsed) {
        implicit builder =>
          { (graph, mesos) =>
            import GraphDSL.Implicits._

            mesos ~> graph.in2
            graph.out2 ~> mesos

            FlowShape(graph.in1, graph.out1)
          }
      }
    }
  }

  private val stateOutputBreakoutFlow: Flow[StateEventOrSnapshot, StateOutput, NotUsed] =
    Flow[StateEventOrSnapshot].prefixAndTail(1).map {
      case (Seq(snapshot), stateEvents) =>
        val stateSnapshot = snapshot match {
          case x: StateSnapshot => x
          case _ => throw new IllegalStateException("First event is allowed to be only a state snapshot")
        }
        val stateUpdates = stateEvents.map {
          case _: StateSnapshot =>
            throw new IllegalStateException("Only the first event is allowed to be a state snapshot")
          case event: StateEvent => event
        }
        (stateSnapshot, stateUpdates)
    }

  private[core] def unconnectedGraph(mesosCallFactory: MesosCalls, podRecordRepository: PodRecordRepository)
    : BidiFlow[SchedulerCommand, StateOutput, MesosEvent, MesosCall, NotUsed] = {
    val schedulerLogicGraph = new SchedulerLogicGraph(mesosCallFactory, loadPodRecords(podRecordRepository))
    BidiFlow.fromGraph {
      GraphDSL.create(schedulerLogicGraph) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
          val stateOutputBreakout = builder.add(stateOutputBreakoutFlow)
          val persistenceStorageFlow = builder.add(persistenceFlow(podRecordRepository))

          schedulerLogic.out ~> persistenceStorageFlow ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { frameResult =>
            frameResult.mesosCalls
          }
          val stateEvents = broadcast.out(1).mapConcat { frameResult =>
            frameResult.stateEvents
          }

          stateEvents ~> stateOutputBreakout

          BidiShape.apply(schedulerLogic.in0, stateOutputBreakout.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }

  private[core] def persistenceFlow(
      podRecordRepository: PodRecordRepository): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
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

  /*
   * We don't start processing any commands until we've finished loading the entire set of podRecords
   * This code delays building a scheduler stage until this podRecord snapshot is available.
   *
   * Block for IO - If the IO call fails or a timeout occurs, we should not make any progress.
   */
  private def loadPodRecords(podRecordRepository: PodRecordRepository): Map[PodId, PodRecord] = {
    // Add error handling (and maybe a retry mechanism).
    Await.result(podRecordRepository.readAll(), schedulerSettings.persistenceLoadTimeout.seconds)
  }

  private def isMultiRoleFramework(frameworkInfo: FrameworkInfo): Boolean =
    frameworkInfo.getCapabilitiesList.asScala.exists(_.getType == FrameworkInfo.Capability.Type.MULTI_ROLE)
}
