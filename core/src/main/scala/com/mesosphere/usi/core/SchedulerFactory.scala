package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.StateSnapshot
import com.mesosphere.usi.core.revive.SuppressReviveHandler
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.PodRecordRepository
import org.apache.mesos.v1.Protos.{DomainInfo, FrameworkID, FrameworkInfo}

trait SchedulerLogicFactory {
  def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph
}

trait PersistenceFlowFactory {
  def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed]
}

trait SuppressReviveFactory {
  def newSuppressReviveHandler: SuppressReviveHandler
}

case class SchedulerFactory(
  client: MesosClient,
  podRecordRepository: PodRecordRepository,
  schedulerSettings: SchedulerSettings,
  frameworkId: FrameworkID,
  initialFrameworkInfo: FrameworkInfo,
  metrics: Metrics,
  mesosCalls: MesosCalls,
  masterDomainInfo: DomainInfo) extends SchedulerLogicFactory with PersistenceFlowFactory with SuppressReviveFactory {

  override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Scheduler.newPersistenceFlow(podRecordRepository, schedulerSettings.persistencePipelineLimit)
  }

  override def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph = {
    new SchedulerLogicGraph(mesosCalls, masterDomainInfo, snapshot, metrics)
  }

  override def newSuppressReviveHandler: SuppressReviveHandler = {
    new SuppressReviveHandler(initialFrameworkInfo, metrics, mesosCalls, schedulerSettings.defaultRole)
  }
}
