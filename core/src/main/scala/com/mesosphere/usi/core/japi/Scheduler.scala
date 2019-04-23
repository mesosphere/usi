package com.mesosphere.usi.core.japi

import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.stream.javadsl.{Flow, Sink, Source}
import akka.stream.{Materializer, javadsl}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.{Scheduler => ScalaScheduler}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

/**
  * Java friendly factory methods of [[com.mesosphere.usi.core.Scheduler]].
  */
object Scheduler {

  type SpecInput = akka.japi.Pair[SpecsSnapshot, javadsl.Source[SpecUpdated, Any]]

  type StateOutput = akka.japi.Pair[StateSnapshot, javadsl.Source[StateEvent, Any]]

  /**
    * Constructs a USI scheduler given an initial pod specs snapshot.
    *
    * @param specsSnapshot The initial snapshot of pod specs.
    * @param client The [[MesosClient]] used to interact with Mesos.
    * @return A [[javadsl]] flow from pod spec updates to state events.
    */
  def fromSnapshot(
      specsSnapshot: SpecsSnapshot,
      client: MesosClient): javadsl.Flow[SpecUpdated, StateOutput, NotUsed] = {
    javadsl.Flow
      .create[SpecUpdated]()
      .via(ScalaScheduler.fromSnapshot(specsSnapshot, client))
      .map { case (taken, tail) => akka.japi.Pair(taken, tail.asJava) }
  }

  /**
    * Constructs a USI scheduler flow to managing pods.
    *
    * The input is a [[akka.japi.Pair]] of [[SpecsSnapshot]] and [[javadsl.Source]] of [[SpecInput]]. The output is a [[akka.japi.Pair]]
    * of [[StateSnapshot]] and [[javadsl.Source]] of [[StateOutput]].
    *
    * @param client The [[MesosClient]] used to interact with Mesos.
    * @return A [[javadsl]] flow from pod specs to state events.
    */
  def fromClient(client: MesosClient): javadsl.Flow[SpecInput, StateOutput, NotUsed] = {
    javadsl.Flow
      .create[SpecInput]()
      .map(pair => pair.first -> pair.second.asScala)
      .via(ScalaScheduler.fromClient(client))
      .map { case (taken, tail) => akka.japi.Pair(taken, tail.asJava) }
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
      mesosFlow: javadsl.Flow[MesosCall, MesosEvent, Any]): javadsl.Flow[SpecInput, StateOutput, NotUsed] = {
    javadsl.Flow
      .create[SpecInput]()
      .map(pair => pair.first -> pair.second.asScala)
      .via(ScalaScheduler.fromFlow(mesosCallFactory, mesosFlow.asScala))
      .map { case (taken, tail) => akka.japi.Pair(taken, tail.asJava) }
  }

  /**
    * Constructs the scheduler as a Sink and Source.
    *
    * This method will materialize the scheduler first, then Sink and Source can be materialized independently.
    *
    * @param specsSnapshot Snapshot of the current specs
    * @return Snapshot of the current state, as well as Source which produces StateEvents and Sink which accepts SpecEvents
    */
  def asSourceAndSink(
      specsSnapshot: SpecsSnapshot,
      client: MesosClient,
      materializer: Materializer): SourceAndSinkResult = {
    val (snap, source, sink) = ScalaScheduler.asSourceAndSink(specsSnapshot, client)(materializer)
    new SourceAndSinkResult(snap.toJava.toCompletableFuture, source.asJava, sink.asJava)
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

  def asFlow(
      specsSnapshot: SpecsSnapshot,
      client: MesosClient,
      materializer: Materializer): CompletableFuture[FlowResult] = {

    implicit val ec = ExecutionContext.Implicits.global

    val flowFuture = ScalaScheduler.asFlow(specsSnapshot, client)(materializer)
    flowFuture.map {
      case (snapshot, flow) =>
        new FlowResult(snapshot, flow.asJava)
    }.toJava.toCompletableFuture

  }

  class FlowResult(snap: StateSnapshot, flow: Flow[SpecUpdated, StateEvent, NotUsed]) {
    def getFlow: Flow[SpecUpdated, StateEvent, NotUsed] = {
      flow
    }

    def getSnapshot: StateSnapshot = {
      snap
    }
  }
}
