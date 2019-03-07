package com.mesosphere.usi.core


import akka.{Done, NotUsed}
import akka.stream.{KillSwitches, Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.models.{SpecEvent, SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}

import scala.concurrent.{Future, Promise}


/**
  * SchedulerAdapter provides a set of simple interfaces to the scheduler flow.
  * @param schedulerFlow
  * @param mat
  */
class SchedulerAdapter(schedulerFlow: Flow[SpecInput, StateOutput, NotUsed])(implicit mat: Materializer) {

  private val extractSnapshot: Flow[SpecEvent, (SpecsSnapshot, Source[SpecUpdated, Any]), NotUsed] = Flow[SpecEvent]
    .prefixAndTail(1) // First event must be a snashot, let's take it
    .map {
      case (Seq(specsSnapshot: SpecsSnapshot), source) =>
        val specUpdatedSource = source.map {
          case specUpdated: SpecUpdated => specUpdated
          case other => throw new IllegalArgumentException(s"expected SpecUpdated but got $other")
        }
        (specsSnapshot, specUpdatedSource)
      case _ =>
        throw new IllegalArgumentException("scheduler spec stream must start with a snapshot")
    }

  private val mergeSnapshot: Flow[(StateSnapshot, Source[StateEvent, Any]), StateEvent, NotUsed] = Flow[StateOutput]
    .flatMapConcat {
      case (snapshot, updates) =>
        updates.prepend(Source.single(snapshot))
    }


  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    * Please note that the first spec event coming to the Sink must be a SpecsSnapshot otherwise the stream will fail.
    *
    * @return Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(): (Source[StateEvent, NotUsed], Sink[SpecEvent, NotUsed]) = {

    val (stateQueue, stateSource) = Source.queue[StateEvent](1, OverflowStrategy.backpressure).preMaterialize()

    val (specQueue, specSink) = Sink.queue[SpecEvent]().preMaterialize()



    Source.maybe
      .prepend {
        Source.unfoldResourceAsync[SpecEvent, SinkQueueWithCancel[SpecEvent]](
          create = () => Future.successful(specQueue),
          read = q => q.pull(),
          close = q => Future.successful {
            q.cancel()
            Done
          })
      }
      .via(extractSnapshot)
      .via(schedulerFlow)
      .via(mergeSnapshot)
      .mapAsync(1)(stateQueue.offer)
      .runWith(Sink.ignore)

    (stateSource, specSink)

  }

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot): (Future[StateSnapshot], Source[StateEvent, Any], Sink[SpecUpdated, Any]) = {

    val (stateQueue, stateSource) = Source.queue[StateEvent](1, OverflowStrategy.backpressure).preMaterialize()

    val (specQueue, specSink) = Sink.queue[SpecUpdated]().preMaterialize()

    val stateSnapshotPromise = Promise[StateSnapshot]()

    Source.maybe
      .prepend {
        val events = Source.unfoldResourceAsync[SpecUpdated, SinkQueueWithCancel[SpecUpdated]](
          create = () => Future.successful(specQueue),
          read = q => q.pull(),
          close = q => Future.successful {
            q.cancel()
            Done
          })

        Source.single(specsSnapshot -> events)
      }
      .via(schedulerFlow)
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
    *
    * Represents the scheduler as a Flow.
    *
    * This method will materialize the scheduler first, then Flow can be materialized independently.
    *
    * @return
    */
  def asFlow(): Flow[SpecEvent, StateEvent, NotUsed] = {
    val (source, sink) = asSourceAndSink()
    Flow.fromSinkAndSourceCoupled(sink, source)
  }


  /**
    *
    * Represents the scheduler as a Flow.
    *
    *
    * This method will materialize the scheduler first, then Flow can be materialized independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asFlow(specsSnapshot: SpecsSnapshot): (Future[StateSnapshot], Flow[SpecUpdated, StateEvent, NotUsed]) = {
    val (snapshot, source, sink) = asSourceAndSink(specsSnapshot)
    snapshot -> Flow.fromSinkAndSourceCoupled(sink, source)
  }

  def asAkkaQueues(overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure): (SourceQueueWithComplete[SpecEvent], SinkQueueWithCancel[StateEvent]) = {

    val (specQueue, specSource) = Source.queue[SpecEvent](1, overflowStrategy).preMaterialize()

    val (stateQueue, stateSink) = Sink.queue[StateEvent]().preMaterialize()

    Source.maybe
      .prepend {
        specSource
      }
      .via(extractSnapshot)
      .via(schedulerFlow)
      .via(mergeSnapshot)
      .runWith(stateSink)

    specQueue -> stateQueue

  }

  /**
    *
    * Represents the scheduler as a SourceQueue and a SinkQueue.
    *
    * This method will materialize the scheduler first, then queues can be used independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asAkkaQueues(specsSnapshot: SpecsSnapshot,
                   overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure):
  (Future[StateSnapshot], SourceQueueWithComplete[SpecUpdated], SinkQueueWithCancel[StateEvent]) = {

    val (specQueue, specSource) = Source.queue[SpecUpdated](1, OverflowStrategy.backpressure).preMaterialize()

    val (stateQueue, stateSink) = Sink.queue[StateEvent]().preMaterialize()

    val stateSnapshotPromise = Promise[StateSnapshot]()

    Source.maybe
      .prepend {
        Source.single(specsSnapshot -> specSource)
      }
      .via(schedulerFlow)
      .flatMapConcat {
        case (snapshot, updates) =>
          stateSnapshotPromise.trySuccess(snapshot)
          updates
      }
      .runWith(stateSink)

    (stateSnapshotPromise.future, specQueue, stateQueue)

  }

}
