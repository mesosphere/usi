package com.mesosphere.usi.core.japi

import java.util.concurrent.CompletableFuture

import akka.stream.{Materializer, javadsl}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{StateEvent, StateSnapshot}
import com.mesosphere.usi.core.{CallerThreadExecutionContext, SchedulerFactory, Scheduler => ScalaScheduler}
import com.mesosphere.usi.repository.PodRecordRepository

import scala.compat.java8.FutureConverters._

/**
  * Java friendly factory methods of [[com.mesosphere.usi.core.Scheduler]].
  */
object Scheduler {

  /**
    * Constructs a USI scheduler flow to managing pods.
    *
    * The input is a [[javadsl.Source]] of [[SchedulerCommand]]. The output is a [[akka.japi.Pair]]
    * of [[StateSnapshot]] and [[javadsl.Source]] of [[StateEvent]].
    *
    * @param factory SchedulerFactory instance; used for constructing dependencies
    * @param client MesosClient instance
    * @param podRecordRepository Repository
    * @return A [[javadsl]] flow from pod specs to state events.
    */
  def fromClient(
      factory: SchedulerFactory,
      client: MesosClient,
      podRecordRepository: PodRecordRepository): CompletableFuture[FlowResult] = {
    ScalaScheduler
      .fromClient(factory, client, podRecordRepository)
      .map { case (snapshot, flow) => new FlowResult(snapshot, flow.asJava) }(CallerThreadExecutionContext.context)
      .toJava
      .toCompletableFuture
  }

  /**
    * Constructs the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(
      factory: SchedulerFactory,
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      materializer: Materializer): CompletableFuture[SourceAndSinkResult] = {
    ScalaScheduler
      .asSourceAndSink(factory, client, podRecordRepository)(materializer)
      .map {
        case (snap, source, sink) =>
          new SourceAndSinkResult(snap, source.asJava, sink.mapMaterializedValue(_.toJava.toCompletableFuture).asJava)
      }(CallerThreadExecutionContext.context)
      .toJava
      .toCompletableFuture
  }

  class SourceAndSinkResult(
      snap: StateSnapshot,
      source: javadsl.Source[StateEvent, NotUsed],
      sink: javadsl.Sink[SchedulerCommand, CompletableFuture[Done]]) {

    def getSource: javadsl.Source[StateEvent, NotUsed] = {
      source
    }

    def getSink: javadsl.Sink[SchedulerCommand, CompletableFuture[Done]] = {
      sink
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }

  class FlowResult(snap: StateSnapshot, flow: javadsl.Flow[SchedulerCommand, StateEvent, NotUsed]) {
    def getFlow: javadsl.Flow[SchedulerCommand, StateEvent, NotUsed] = {
      flow
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }
}
