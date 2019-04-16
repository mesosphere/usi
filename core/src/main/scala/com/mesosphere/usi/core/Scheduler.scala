package com.mesosphere.usi.core

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink, SinkQueueWithCancel, Source}
import akka.stream.{BidiShape, FlowShape, KillSwitches, Materializer, OverflowStrategy, QueueOfferResult}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.models.{SpecEvent, SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._
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

  def fromSnapshot(specsSnapshot: SpecsSnapshot, client: MesosClient): Flow[SpecUpdated, StateOutput, NotUsed] =
    Flow[SpecUpdated].prefixAndTail(0).map { case (_, rest) => specsSnapshot -> rest }.via(fromClient(client))

  def asFlow(specsSnapshot: SpecsSnapshot, client: MesosClient)(
      implicit materializer: Materializer): Future[(StateSnapshot, Flow[SpecUpdated, StateEvent, NotUsed])] = {

    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global //only for ultra-fast non-blocking onComplete

    val (snap, source, sink) = asSourceAndSink(specsSnapshot, client)

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
  def asSourceAndSink(specsSnapshot: SpecsSnapshot, client: MesosClient)(
      implicit mat: Materializer): (Future[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]) = {
    val flow = fromClient(client)
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
