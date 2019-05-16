package com.mesosphere.usi.core.japi

import java.util.concurrent.CompletableFuture

import akka.stream.javadsl.{Flow, Sink, Source}
import akka.stream.{Materializer, javadsl}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{SchedulerCommand, StateEvent, StateEventOrSnapshot, StateSnapshot}
import com.mesosphere.usi.core.{CallerThreadExecutionContext, Scheduler => ScalaScheduler}
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

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
    * @param client The [[MesosClient]] used to interact with Mesos.
    * @return A [[javadsl]] flow from pod specs to state events.
    */
  def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      schedulerSettings: SchedulerSettings): CompletableFuture[FlowResult] = {
    val flow = Flow.fromSinkAndSource(client.mesosSink, client.mesosSource)
    fromFlow(client.calls, podRecordRepository, flow, schedulerSettings)
  }

  /**
    * Constructs a USI scheduler flow to managing pods.
    *
    * See [[Scheduler.fromClient()]] for a simpler constructor.
    *
    * @param mesosCallFactory A factory for construct [[MesosCall]]s.
    * @param mesosFlow A flow from [[MesosCall]]s to [[MesosEvent]]s.
    * @return A [[javadsl]] flow from pod specs to state events.
    */
  def fromFlow(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      mesosFlow: javadsl.Flow[MesosCall, MesosEvent, NotUsed],
      schedulerSettings: SchedulerSettings): CompletableFuture[FlowResult] = {

    ScalaScheduler
      .fromFlow(mesosCallFactory, podRecordRepository, mesosFlow.asScala, schedulerSettings)
      .map {
        case (snapshot, flow) =>
          new FlowResult(snapshot, flow.asJava)
      }(CallerThreadExecutionContext.context)
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
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      schedulerSettings: SchedulerSettings,
      materializer: Materializer): CompletableFuture[SourceAndSinkResult] = {
    ScalaScheduler
      .asSourceAndSink(client, podRecordRepository, schedulerSettings)(materializer)
      .map {
        case (snap, source, sink) =>
          new SourceAndSinkResult(snap, source.asJava, sink.mapMaterializedValue(_.toJava.toCompletableFuture).asJava)
      }(CallerThreadExecutionContext.context)
      .toJava
      .toCompletableFuture
  }

  class SourceAndSinkResult(
      snap: StateSnapshot,
      source: Source[StateEvent, NotUsed],
      sink: Sink[SchedulerCommand, CompletableFuture[Done]]) {

    def getSource: Source[StateEvent, NotUsed] = {
      source
    }

    def getSink: Sink[SchedulerCommand, CompletableFuture[Done]] = {
      sink
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }

  class FlowResult(snap: StateSnapshot, flow: Flow[SchedulerCommand, StateEventOrSnapshot, NotUsed]) {
    def getFlow: Flow[SchedulerCommand, StateEventOrSnapshot, NotUsed] = {
      flow
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }
}
