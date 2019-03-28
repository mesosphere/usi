package com.mesosphere.usi.core.javaapi

import akka.NotUsed
import akka.stream.javadsl
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.{Scheduler => ScalaScheduler}
import com.mesosphere.usi.core.models.{SpecUpdated, SpecsSnapshot, StateEvent, StateSnapshot}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

object Scheduler {

  type SpecInput = akka.japi.Pair[SpecsSnapshot, javadsl.Source[SpecUpdated, Any]]

  type StateOutput = akka.japi.Pair[StateSnapshot, javadsl.Source[StateEvent, Any]]

  def fromClient(client: MesosClient): javadsl.Flow[SpecInput, StateOutput, NotUsed] = {
    javadsl.Flow
      .create[SpecInput]()
      .map(pair => pair.first -> pair.second.asScala)
      .via(ScalaScheduler.fromClient(client))
      .map { case (taken, tail) => akka.japi.Pair(taken, tail.asJava) }
  }

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
