package com.mesosphere.usi.core.helpers

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{PodSpecUpdatedEvent, StateSnapshot}
import com.mesosphere.usi.core.revive.SuppressReviveHandler
import com.mesosphere.usi.core.util.DurationConverters
import com.mesosphere.usi.core._
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.PodRecordRepository
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import org.apache.mesos.v1.Protos.{DomainInfo, FrameworkID, FrameworkInfo}
import org.apache.mesos.v1.scheduler.Protos
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

case class MockedFactory(
    podRecordRepository: PodRecordRepository = InMemoryPodRecordRepository(),
    schedulerSettings: SchedulerSettings =
      SchedulerSettings.load().withDebounceReviveInterval(DurationConverters.toJava(50.millis)),
    frameworkId: FrameworkID = MesosMock.mockFrameworkId,
    frameworkInfo: FrameworkInfo = MesosMock.mockFrameworkInfo(),
    masterDomainInfo: DomainInfo = MesosMock.masterDomainInfo,
    metrics: Metrics = DummyMetrics
)(implicit ec: ExecutionContext)
    extends SchedulerLogicFactory
    with PersistenceFlowFactory
    with SuppressReviveFactory
    with MesosFlowFactory {

  val mesosCalls = new MesosCalls(frameworkId: FrameworkID)
  override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Scheduler.newPersistenceFlow(podRecordRepository, schedulerSettings.persistencePipelineLimit)
  }

  override def newMesosFlow: Flow[MesosCall, Protos.Event, NotUsed] = ???

  override def loadSnapshot(): Future[StateSnapshot] = {
    podRecordRepository
      .readAll()
      .map { podRecords =>
        StateSnapshot(podRecords = podRecords.values.toSeq, agentRecords = Nil)
      }
  }

  override def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph = {
    new SchedulerLogicGraph(mesosCalls, masterDomainInfo, snapshot, metrics)
  }

  override def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, MesosCall, NotUsed] = {
    new SuppressReviveHandler(
      frameworkInfo,
      frameworkId,
      metrics,
      mesosCalls,
      debounceReviveInterval = DurationConverters.toScala(schedulerSettings.debounceReviveInterval)
    ).flow
  }
}
