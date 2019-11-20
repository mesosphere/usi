package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.{FanInShape2, Graph}
import akka.stream.scaladsl.Flow
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{PodSpecUpdatedEvent, StateEvent, StateSnapshot}
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.revive.SuppressReviveHandler
import com.mesosphere.usi.core.util.DurationConverters
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.scheduler.Protos
import org.apache.mesos.v1.scheduler.Protos.{Event => MesosEvent}
import scala.concurrent.Future

trait SchedulerLogicFactory {
  private[usi] def newSchedulerLogicGraph(
      snapshot: StateSnapshot): Graph[FanInShape2[SchedulerCommand, MesosEvent, SchedulerEvents], NotUsed]
}

trait PersistenceFlowFactory {
  private[usi] def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed]
}

trait SuppressReviveFactory {
  def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, Protos.Call, NotUsed]
}

case class SchedulerFactory(
    client: MesosClient,
    podRecordRepository: PodRecordRepository,
    schedulerSettings: SchedulerSettings,
    metrics: Metrics)
    extends SchedulerLogicFactory
    with PersistenceFlowFactory
    with SuppressReviveFactory {

  def newSchedulerFlow(): Future[(StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed])] =
    Scheduler.fromClient(this, client, podRecordRepository)
  override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Scheduler.newPersistenceFlow(podRecordRepository, schedulerSettings.persistencePipelineLimit)
  }

  override def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph = {
    new SchedulerLogicGraph(client.calls, client.masterInfo.getDomain, snapshot, metrics)
  }

  override def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, Protos.Call, NotUsed] = {
    new SuppressReviveHandler(
      client.frameworkInfo,
      client.frameworkId,
      metrics,
      client.calls,
      debounceReviveInterval = DurationConverters.toScala(schedulerSettings.debounceReviveInterval)
    ).flow
  }
}
