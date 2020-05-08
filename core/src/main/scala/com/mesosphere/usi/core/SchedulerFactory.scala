package com.mesosphere.usi.core

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.stream.{FanInShape2, Graph, Materializer}
import akka.stream.scaladsl.{Flow, RestartFlow, Sink}
import com.mesosphere.mesos.client.{CredentialsProvider, MesosCalls, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{PodSpecUpdatedEvent, StateEvent, StateSnapshot}
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.revive.SuppressReviveHandler
import com.mesosphere.usi.core.util.DurationConverters
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.PodRecordRepository
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

private[usi] trait SchedulerLogicFactory {
  private[usi] def newSchedulerLogicGraph(
      snapshot: StateSnapshot): Graph[FanInShape2[SchedulerCommand, MesosEvent, SchedulerEvents], NotUsed]

  val frameworkInfo: FrameworkInfo
}

private[usi] trait PersistenceFlowFactory {
  private[usi] def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed]

  private[usi] def loadSnapshot(): Future[StateSnapshot]
}

private[usi] trait SuppressReviveFactory {
  def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, Protos.Call, NotUsed]
}

private[usi] trait MesosFlowFactory {
  def newMesosFlow: Flow[MesosCall, MesosEvent, NotUsed]
}

/**
  * Factory used for instantiating the scheduler stream.
  *
  * @param client Reference to the Mesos client
  * @param podRecordRepository The persistence layer for podRecords / reservationRecords
  * @param schedulerSettings Settings, usually loaded from application.conf
  * @param metrics Metrics
  */
class SchedulerFactory private (
    clientSettings: MesosClientSettings,
    frameworkInfo: FrameworkInfo,
    authorization: Option[CredentialsProvider],
    podRecordRepository: PodRecordRepository,
    schedulerSettings: SchedulerSettings,
    metrics: Metrics)(implicit ec: ExecutionContext, system: ActorSystem, materializer: Materializer)
    extends SchedulerLogicFactory
    with PersistenceFlowFactory
    with SuppressReviveFactory
    with MesosFlowFactory
    with StrictLogging {

  override def newMesosFlow: Flow[MesosCall, MesosEvent, NotUsed] =
    RestartFlow.withBackoff(5.seconds, 30.seconds, 0.2, 10) { () =>
      // TODO: Inject client factory
      val flow = MesosClient(clientSettings, frameworkInfo, authorization).map { client =>
        Flow.fromSinkAndSourceCoupled(logMesosCallException(client.mesosSink), client.mesosSource)
      }.runWith(Sink.head)
      Flow.futureFlow(flow)
    }

  def newSchedulerFlow(): Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] =
    Scheduler.asFlow(this)
  override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Scheduler.newPersistenceFlow(podRecordRepository, schedulerSettings.persistencePipelineLimit)
  }

  override def loadSnapshot(): Future[StateSnapshot] = {
    podRecordRepository
      .readAll()
      .map { podRecords =>
        StateSnapshot(podRecords = podRecords.values.toSeq, agentRecords = Nil)
      }
  }

  override def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph = {
    new SchedulerLogicGraph(new MesosCalls(), client.masterInfo.getDomain, snapshot, metrics)
  }

  override def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, Protos.Call, NotUsed] = {
    new SuppressReviveHandler(
      frameworkInfo,
      metrics,
      new MesosCalls(),
      debounceReviveInterval = DurationConverters.toScala(schedulerSettings.debounceReviveInterval)
    ).flow
  }

  private def logMesosCallException[T](s: Sink[T, Future[Done]]): Sink[T, NotUsed] = {
    s.mapMaterializedValue { f =>
      f.failed.foreach { ex =>
        logger.error("Mesos client hanging up due to error in stream", ex)
      }(CallerThreadExecutionContext.context)
      NotUsed
    }
  }
}

object SchedulerFactory {

  def apply(
      clientSettings: MesosClientSettings,
      frameworkInfo: FrameworkInfo,
      authorization: Option[CredentialsProvider],
      podRecordRepository: PodRecordRepository,
      schedulerSettings: SchedulerSettings,
      metrics: Metrics)(implicit ec: ExecutionContext, system: ActorSystem, materializer: Materializer) =
    new SchedulerFactory(clientSettings, frameworkInfo, authorization, podRecordRepository, schedulerSettings, metrics)

  def create(
      clientSettings: MesosClientSettings,
      frameworkInfo: FrameworkInfo,
      authorization: Option[CredentialsProvider],
      podRecordRepository: PodRecordRepository,
      schedulerSettings: SchedulerSettings,
      metrics: Metrics,
      ec: ExecutionContext,
      system: ActorSystem,
      materializer: Materializer) =
    new SchedulerFactory(clientSettings, frameworkInfo, authorization, podRecordRepository, schedulerSettings, metrics)(
      ec,
      system,
      materializer)
}
