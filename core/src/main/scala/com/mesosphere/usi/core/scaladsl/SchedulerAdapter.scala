package com.mesosphere.usi.core.scaladsl

import akka.stream.scaladsl.{Flow, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.models._

import scala.concurrent.{Future, Promise}

/**
  * SchedulerAdapter provides a set of simple interfaces to the scheduler flow.
  * @param schedulerFlow
  * @param mat
  */
class SchedulerAdapter(schedulerFlow: Flow[SpecInput, StateOutput, NotUsed])(implicit mat: Materializer) {

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty)
    : (Future[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]) = {

    val (stateQueue, stateSource) = Source.queue[StateEvent](1, OverflowStrategy.backpressure).preMaterialize()

    val (specQueue, specSink) = Sink.queue[SpecUpdated]().preMaterialize()

    val stateSnapshotPromise = Promise[StateSnapshot]()

    Source.maybe.prepend {
      val events = Source.unfoldResourceAsync[SpecUpdated, SinkQueueWithCancel[SpecUpdated]](
        create = () => Future.successful(specQueue),
        read = q => q.pull(),
        close = q =>
          Future.successful {
            q.cancel()
            Done
        })

      Source.single(specsSnapshot -> events)
    }.via(schedulerFlow)
      .flatMapConcat {
        case (snapshot, updates) =>
          stateSnapshotPromise.trySuccess(snapshot)
          updates
      }
      .mapAsync(1)(stateQueue.offer)
      .runWith(Sink.ignore)

    (stateSnapshotPromise.future, stateSource, specSink)

  }

  /**
    * Represents the scheduler as a Flow.
    *
    * This method will materialize the scheduler first, then Flow can be materialized independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asFlow(specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty)
    : (Future[StateSnapshot], Flow[SpecUpdated, StateEvent, NotUsed]) = {
    val (snapshot, source, sink) = asSourceAndSink(specsSnapshot)
    snapshot -> Flow.fromSinkAndSourceCoupled(sink, source)
  }

  /**
    * Represents the scheduler as a SourceQueue and a SinkQueue.
    *
    * This method will materialize the scheduler first, then queues can be used independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asAkkaQueues(
      specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty,
      overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure)
    : (Future[StateSnapshot], SourceQueueWithComplete[SpecUpdated], SinkQueueWithCancel[StateEvent]) = {

    val (specQueue, specSource) = Source.queue[SpecUpdated](1, overflowStrategy).preMaterialize()

    val (stateQueue, stateSink) = Sink.queue[StateEvent]().preMaterialize()

    val stateSnapshotPromise = Promise[StateSnapshot]()

    Source.maybe.prepend {
      Source.single(specsSnapshot -> specSource)
    }.via(schedulerFlow)
      .flatMapConcat {
        case (snapshot, updates) =>
          stateSnapshotPromise.trySuccess(snapshot)
          updates
      }
      .runWith(stateSink)

    (stateSnapshotPromise.future, specQueue, stateQueue)

  }

}
