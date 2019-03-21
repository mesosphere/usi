package com.mesosphere.usi.core.scalaapi

import akka.stream.scaladsl.{Flow, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import akka.stream._
import akka.{Done, NotUsed}
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.models._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * SchedulerAdapter provides a set of simple interfaces to the scheduler flow.
  * @param schedulerFlow
  * @param mat
  */
class SchedulerAdapter(schedulerFlow: Flow[SpecInput, StateOutput, NotUsed])(
    implicit mat: Materializer,
    ec: ExecutionContext) {

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently, but only once.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty)
    : (Future[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]) = {

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
    }
      .via(schedulerFlow)
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

  /**
    * Represents the scheduler as a SourceQueue and a SinkQueue.
    *
    * @see https://doc.akka.io/api/akka/current/akka/stream/scaladsl/SourceQueue.html
    *      https://doc.akka.io/api/akka/current/akka/stream/scaladsl/SinkQueue.html
    *
    * This method will materialize the scheduler first, then queues can be used independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asAkkaQueues(
      specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty,
      overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure,
      bufferSize: Int = 1)
    : (Future[StateSnapshot], SourceQueueWithComplete[SpecUpdated], SinkQueueWithCancel[StateEvent]) = {

    val (specQueue, specSource) = Source.queue[SpecUpdated](bufferSize, overflowStrategy).preMaterialize()

    val (stateQueue, stateSink) = Sink.queue[StateEvent]().preMaterialize()

    val (snapshot, source, sink) = asSourceAndSink(specsSnapshot)

    specSource.runWith(sink)
    source.runWith(stateSink)

    (snapshot, specQueue, stateQueue)
  }

}
