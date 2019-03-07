package com.mesosphere.usi.core.javadsl

import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.{Done, NotUsed}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, scaladsl}
import akka.stream.javadsl.{Flow, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.scaladsl.{SchedulerAdapter => ScalaSchedulerAdapter}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}

import scala.compat.java8.FutureConverters._
import akka.japi.tuple.Tuple3
import akka.japi.Pair

class SchedulerAdapter(schedulerFlow: scaladsl.Flow[SpecInput, StateOutput, NotUsed], mat: Materializer) {

  private implicit val materializer: Materializer = mat

  private val delegate = new ScalaSchedulerAdapter(schedulerFlow)

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty)
    : Tuple3[CompletableFuture[StateSnapshot], Source[StateEvent, Any], Sink[SpecUpdated, Any]] = {
    val (snap, source, sink) = delegate.asSourceAndSink(specsSnapshot)
    Tuple3(snap.toJava.toCompletableFuture, source.asJava, sink.asJava)
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
  def asFlow(specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty)
    : Pair[CompletableFuture[StateSnapshot], Flow[SpecUpdated, StateEvent, NotUsed]] = {
    val (snap, flow) = delegate.asFlow(specsSnapshot)
    Pair(snap.toJava.toCompletableFuture, flow.asJava)
  }

  /**
    *
    * Represents the scheduler as a SourceQueue and a SinkQueue.
    *
    * This method will materialize the scheduler first, then queues can be used independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asAkkaQueues(
      specsSnapshot: SpecsSnapshot = SpecsSnapshot.empty,
      overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure): Tuple3[
    CompletableFuture[StateSnapshot],
    SourceQueueWithComplete[SpecUpdated],
    SinkQueueWithCancel[StateEvent]] = {
    val (snap, specQueue, stateQueue) = delegate.asAkkaQueues(specsSnapshot, overflowStrategy)
    val javaSpecQueue = new SourceQueueWithComplete[SpecUpdated] {
      override def complete(): Unit = specQueue.complete()
      override def fail(ex: Throwable): Unit = specQueue.fail(ex)
      override def watchCompletion(): CompletionStage[Done] = specQueue.watchCompletion().toJava
      override def offer(elem: SpecUpdated): CompletionStage[QueueOfferResult] = specQueue.offer(elem).toJava
    }
    val javaStateQueue = new SinkQueueWithCancel[StateEvent] {
      override def cancel(): Unit = stateQueue.cancel()
      override def pull(): CompletionStage[Optional[StateEvent]] =
        stateQueue
          .pull()
          .map(scalaOpt => Optional.ofNullable(scalaOpt.orNull))(concurrent.ExecutionContext.Implicits.global)
          .toJava
    }
    Tuple3(snap.toJava.toCompletableFuture, javaSpecQueue, javaStateQueue)
  }

}
