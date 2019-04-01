package com.mesosphere.usi.core.japi

import akka.NotUsed
import akka.stream.javadsl
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.{Scheduler => ScalaScheduler}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

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
    * The input is a [[akka.japi.Pair]] of [[SpecsSnapshot]] and [[javadsl.Source]]. The output is a [[akka.japi.Pair]]
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
}
