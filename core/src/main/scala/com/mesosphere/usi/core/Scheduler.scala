package com.mesosphere.usi.core

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink, SinkQueueWithCancel, Source}
import akka.stream.{BidiShape, FlowShape, KillSwitches, Materializer, OverflowStrategy, QueueOfferResult}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.PodId
import com.mesosphere.usi.core.models.PodRecord
import com.mesosphere.usi.core.models.{
  PodRecordUpdated,
  SpecEvent,
  SpecUpdated,
  SpecsSnapshot,
  StateEvent,
  StateSnapshot
}
import com.mesosphere.usi.repository.PodRecordRepository
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

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

  private val schedulerSettings = SchedulerSettings.fromConfig(ConfigFactory.load().getConfig("scheduler"))

  def asFlow(specsSnapshot: SpecsSnapshot, client: MesosClient, podRecordRepository: PodRecordRepository)(
      implicit materializer: Materializer): Future[(StateSnapshot, Flow[SpecUpdated, StateEvent, NotUsed])] = {

    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global //only for ultra-fast non-blocking map

    val (snap, source, sink) = asSourceAndSink(specsSnapshot, client, podRecordRepository)

    snap.map { snapshot =>
      (snapshot, Flow.fromSinkAndSourceCoupled(sink, source))
    }
  }

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot, client: MesosClient, podRecordRepository: PodRecordRepository)(
      implicit mat: Materializer): (Future[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]) = {
    val flow = fromClient(client, podRecordRepository)
    asSourceAndSink(specsSnapshot, flow)(mat)
  }

  def asSourceAndSink(specsSnapshot: SpecsSnapshot, schedulerFlow: Flow[SpecInput, StateOutput, NotUsed])(
      implicit mat: Materializer): (Future[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]) = {

    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global //only for ultra-fast non-blocking onComplete

    val (stateQueue, stateSource) = Source.queue[StateEvent](1, OverflowStrategy.backpressure).preMaterialize()

    val (specQueue, specSink) = Sink.queue[SpecUpdated]().preMaterialize()

    val stateSnapshotPromise = Promise[StateSnapshot]()

    val killSwitch = KillSwitches.shared("SchedulerAdapter.asSourceAndSink")

    // We need to handle the case when the source is canceled or failed
    stateQueue.watchCompletion().onComplete {
      case Success(_) =>
        killSwitch.shutdown()
      case Failure(cause) =>
        killSwitch.abort(cause)
    }

    def sourceFromSinkQueue[T](queue: SinkQueueWithCancel[T]): Source[T, NotUsed] = {
      Source
        .unfoldResourceAsync[T, SinkQueueWithCancel[T]](
          create = () => Future.successful(queue),
          read = queue => queue.pull(),
          close = queue =>
            Future.successful {
              killSwitch.shutdown()
              queue.cancel()
              Done
          })
    }

    Source.maybe.prepend {
      val events = sourceFromSinkQueue(specQueue)
        .watchTermination() {
          case (_, completionSignal) =>
            completionSignal.onComplete {
              case Success(_) =>
                killSwitch.shutdown()
              case Failure(cause) =>
                killSwitch.abort(cause)
            }
        }

      Source.single(specsSnapshot -> events)
    }.via(schedulerFlow)
      .flatMapConcat {
        case (snapshot, updates) =>
          stateSnapshotPromise.trySuccess(snapshot)
          updates.watchTermination() {
            case (_, cancellationSignal) =>
              cancellationSignal.onComplete {
                case Success(_) =>
                  killSwitch.shutdown()
                case Failure(cause) =>
                  killSwitch.abort(cause)
              }
          }
      }
      .mapAsync(1)(stateQueue.offer)
      .map {
        case QueueOfferResult.Enqueued =>
        case QueueOfferResult.QueueClosed =>
          killSwitch.shutdown()
        case QueueOfferResult.Failure(cause) =>
          killSwitch.abort(cause)
        case QueueOfferResult.Dropped => // we shouldn't receive that at all because OverflowStrategy.backpressure
          throw new RuntimeException("Unexpected QueueOfferResult.Dropped element")
      }
      .runWith(Sink.ignore)

    val sourceWithKillSwitch = stateSource
      .watchTermination() {
        case (materializedValue, cancellationSignal) =>
          cancellationSignal.onComplete {
            case Success(_) =>
              killSwitch.shutdown()
            case Failure(cause) =>
              killSwitch.abort(cause)
          }
          materializedValue
      }
      .via(killSwitch.flow)

    val sinkWithKillSwitch = Flow[SpecUpdated]
      .watchTermination() {
        case (materializedValue, cancellationSignal) =>
          cancellationSignal.onComplete {
            case Success(_) =>
              killSwitch.shutdown()
            case Failure(cause) =>
              killSwitch.abort(cause)
          }
          materializedValue
      }
      .via(killSwitch.flow)
      .to(specSink)

    (stateSnapshotPromise.future, sourceWithKillSwitch, sinkWithKillSwitch)

  }

  def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository): Flow[SpecInput, StateOutput, NotUsed] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    fromFlow(client.calls, podRecordRepository, Flow.fromSinkAndSource(client.mesosSink, client.mesosSource))
  }

  def fromFlow(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SpecInput, StateOutput, NotUsed] = {
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

  private val stateOutputBreakoutFlow: Flow[StateEvent, StateOutput, NotUsed] = Flow[StateEvent].prefixAndTail(1).map {
    case (Seq(snapshot), stateEvents) =>
      val stateSnapshot = snapshot match {
        case x: StateSnapshot => x
        case _ => throw new IllegalStateException("First event is allowed to be only a state snapshot")
      }
      val stateUpdates = stateEvents.map {
        case _: StateSnapshot =>
          throw new IllegalStateException("Only the first event is allowed to be a state snapshot")
        case event => event
      }
      (stateSnapshot, stateUpdates)
  }

  private[core] def unconnectedGraph(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository): BidiFlow[SpecInput, StateOutput, MesosEvent, MesosCall, NotUsed] = {
    val schedulerLogicGraph = new SchedulerLogicGraph(mesosCallFactory, loadPodRecords(podRecordRepository))
    BidiFlow.fromGraph {
      GraphDSL.create(schedulerLogicGraph) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
          val specInputFlattening = builder.add(specInputFlatteningFlow)
          val stateOutputBreakout = builder.add(stateOutputBreakoutFlow)

          val persistenceStorageFlow = builder.add(persistenceFlow(podRecordRepository))
          specInputFlattening ~> schedulerLogic.in0
          schedulerLogic.out ~> persistenceStorageFlow ~> broadcast.in

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
      case PodRecordUpdated(_, Some(podRecord)) =>
        () =>
          podRecordRepository.store(podRecord).map(_ => None)(CallerThreadExecutionContext.context)
      case PodRecordUpdated(podId, None) =>
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
    // Add better error handling (and maybe a retry mechanism).
    Await.result(podRecordRepository.readAll(), schedulerSettings.persistenceLoadTimeout.seconds)
  }

  private def isMultiRoleFramework(frameworkInfo: FrameworkInfo): Boolean =
    frameworkInfo.getCapabilitiesList.asScala.exists(_.getType == FrameworkInfo.Capability.Type.MULTI_ROLE)
}
