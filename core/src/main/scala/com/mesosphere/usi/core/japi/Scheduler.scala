package com.mesosphere.usi.core.japi

import java.util.concurrent.CompletableFuture

import akka.stream.javadsl.{Flow, Sink, Source}
import akka.stream.{Materializer, javadsl, scaladsl}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.models.{SchedulerCommand, StateEvent, StateSnapshot}
import com.mesosphere.usi.core.{Scheduler => ScalaScheduler}
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

/**
  * Java friendly factory methods of [[com.mesosphere.usi.core.Scheduler]].
  */
object Scheduler {

  type StateOutput = akka.japi.Pair[StateSnapshot, javadsl.Source[StateEvent, Any]]

  /**
    * Constructs a USI scheduler flow to managing pods.
    *
    * The input is a [[javadsl.Source]] of [[SchedulerCommand]]. The output is a [[akka.japi.Pair]]
    * of [[StateSnapshot]] and [[javadsl.Source]] of [[StateOutput]].
    *
    * @param client The [[MesosClient]] used to interact with Mesos.
    * @return A [[javadsl]] flow from pod specs to state events.
    */
  def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository): javadsl.Flow[SchedulerCommand, StateOutput, NotUsed] = {
    val flow = Flow.fromSinkAndSource(client.mesosSink, client.mesosSource)
    fromFlow(client.calls, podRecordRepository, flow)
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
      mesosFlow: javadsl.Flow[MesosCall, MesosEvent, NotUsed]): javadsl.Flow[SchedulerCommand, StateOutput, NotUsed] = {
    javadsl.Flow
      .create[SchedulerCommand]()
      .via(ScalaScheduler.fromFlow(mesosCallFactory, podRecordRepository, mesosFlow.asScala))
      .map {
        case (stateSnapshot: StateSnapshot, stateEvents: scaladsl.Source[StateEvent, Any]) =>
          akka.japi.Pair(stateSnapshot, stateEvents.asJava)
      }
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
      materializer: Materializer): SourceAndSinkResult = {
    val (snap, source, sink) = ScalaScheduler.asSourceAndSink(client, podRecordRepository)(materializer)
    new SourceAndSinkResult(
      snap.toJava.toCompletableFuture,
      source.asJava,
      sink.mapMaterializedValue(_.toJava.toCompletableFuture).asJava)
  }

  class SourceAndSinkResult(
      snap: CompletableFuture[StateSnapshot],
      source: Source[StateEvent, NotUsed],
      sink: Sink[SchedulerCommand, CompletableFuture[Done]]) {
    def getSource: Source[StateEvent, NotUsed] = {
      source
    }

    def getSink: Sink[SchedulerCommand, CompletableFuture[Done]] = {
      sink
    }

    def getSnapshot: CompletableFuture[StateSnapshot] = {
      snap
    }
  }

  def asFlow(
      client: MesosClient,
      podRecordRepository: PodRecordRepository,
      materializer: Materializer): CompletableFuture[FlowResult] = {

    implicit val ec = ExecutionContext.Implicits.global

    val flowFuture = ScalaScheduler.asFlow(client, podRecordRepository)(materializer)
    flowFuture.map {
      case (snapshot, flow) =>
        new FlowResult(snapshot, flow.asJava)
    }.toJava.toCompletableFuture
  }

  class FlowResult(snap: StateSnapshot, flow: Flow[SchedulerCommand, StateEvent, NotUsed]) {
    def getFlow: Flow[SchedulerCommand, StateEvent, NotUsed] = {
      flow
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }
}
