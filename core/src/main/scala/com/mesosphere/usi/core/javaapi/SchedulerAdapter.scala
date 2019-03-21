package com.mesosphere.usi.core.javaapi

import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.{Done, NotUsed}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, scaladsl}
import akka.stream.javadsl.{Flow, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.scalaapi.{SchedulerAdapter => ScalaSchedulerAdapter}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}

import scala.compat.java8.FutureConverters._
import akka.japi.tuple.Tuple3
import akka.japi.Pair

import scala.concurrent.ExecutionContext

class SchedulerAdapter(
    schedulerFlow: scaladsl.Flow[SpecInput, StateOutput, NotUsed],
    mat: Materializer,
    ec: ExecutionContext) {

  private implicit val materializer: Materializer = mat
  private implicit val executionContext: ExecutionContext = ec

  private val delegate = new ScalaSchedulerAdapter(schedulerFlow)

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(specsSnapshot: SpecsSnapshot)
    : Tuple3[CompletableFuture[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]] = {
    val (snap, source, sink) = delegate.asSourceAndSink(specsSnapshot)
    Tuple3(snap.toJava.toCompletableFuture, source.asJava, sink.asJava)
  }

  def asSourceAndSink(): Tuple3[CompletableFuture[StateSnapshot], Source[StateEvent, NotUsed], Sink[SpecUpdated, NotUsed]] = {
    asSourceAndSink(SpecsSnapshot.empty)
  }

  /**
    * Represents the scheduler as a SourceQueue and a SinkQueue.
    *
    * This method will materialize the scheduler first, then queues can be used independently.
    * @param specsSnapshot Snapshot of the current specs
    * @return
    */
  def asAkkaQueues(
      specsSnapshot: SpecsSnapshot,
      overflowStrategy: OverflowStrategy): Tuple3[
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

  def asAkkaQueues(): Tuple3[
    CompletableFuture[StateSnapshot],
    SourceQueueWithComplete[SpecUpdated],
    SinkQueueWithCancel[StateEvent]] = {
    asAkkaQueues(SpecsSnapshot.empty, OverflowStrategy.backpressure)
  }

}
