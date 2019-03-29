package com.mesosphere.usi.core.javaapi

import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.{Done, NotUsed}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, scaladsl}
import akka.stream.javadsl._
import com.mesosphere.usi.core.Scheduler.{SpecInput, StateOutput}
import com.mesosphere.usi.core.scalaapi.{SchedulerAdapter => ScalaSchedulerAdapter}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}

import scala.compat.java8.FutureConverters._

import scala.concurrent.ExecutionContext

class SchedulerAdapter(mat: Materializer, ec: ExecutionContext) {

  private implicit val materializer: Materializer = mat
  private implicit val executionContext: ExecutionContext = ec

  /**
    * Represents the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(
      specsSnapshot: SpecsSnapshot,
      schedulerFlow: scaladsl.Flow[SpecInput, StateOutput, NotUsed]): SourceAndSinkResult = {
    val (snap, source, sink) = ScalaSchedulerAdapter.asSourceAndSink(specsSnapshot, schedulerFlow)
    new SourceAndSinkResult(snap.toJava.toCompletableFuture, source.asJava, sink.asJava)
  }

  def asFlow(
      specsSnapshot: SpecsSnapshot,
      schedulerFlow: scaladsl.Flow[SpecInput, StateOutput, NotUsed]): FlowResult = {
    val (snap, flow) = ScalaSchedulerAdapter.asFlow(specsSnapshot, schedulerFlow)
    new FlowResult(snap.toJava.toCompletableFuture, flow.asJava)
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
      overflowStrategy: OverflowStrategy,
      schedulerFlow: scaladsl.Flow[SpecInput, StateOutput, NotUsed]): QueuesResult = {
    val (snap, specQueue, stateQueue) =
      ScalaSchedulerAdapter.asAkkaQueues(specsSnapshot, schedulerFlow, overflowStrategy)
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
    new QueuesResult(snap.toJava.toCompletableFuture, javaSpecQueue, javaStateQueue)
  }

  class SourceAndSinkResult(
      snap: CompletableFuture[StateSnapshot],
      source: Source[StateEvent, NotUsed],
      sink: Sink[SpecUpdated, NotUsed]) {
    def getSource: Source[StateEvent, NotUsed] = {
      source
    }

    def getSink: Sink[SpecUpdated, NotUsed] = {
      sink
    }

    def getSnapshot: CompletableFuture[StateSnapshot] = {
      snap
    }
  }

  class FlowResult(snap: CompletableFuture[StateSnapshot], flow: Flow[SpecUpdated, StateEvent, NotUsed]) {
    def getFlow: Flow[SpecUpdated, StateEvent, NotUsed] = {
      flow
    }

    def getSnapshot: CompletableFuture[StateSnapshot] = {
      snap
    }
  }

  class QueuesResult(
      snap: CompletableFuture[StateSnapshot],
      sourceQueue: SourceQueueWithComplete[SpecUpdated],
      sinkQueue: SinkQueueWithCancel[StateEvent]) {
    def getSourceQueue: SourceQueueWithComplete[SpecUpdated] = {
      sourceQueue
    }

    def getSinkQueue: SinkQueueWithCancel[StateEvent] = {
      sinkQueue
    }

    def getSnapshot: CompletableFuture[StateSnapshot] = {
      snap
    }
  }

}
