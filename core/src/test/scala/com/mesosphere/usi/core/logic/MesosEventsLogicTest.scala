package com.mesosphere.usi.core.logic

import com.google.protobuf.ByteString
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.SchedulerState
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.faultdomain.{HomeRegionFilter, RegionFilter}
import com.mesosphere.usi.core.models.{PodId, PodStatus, PodStatusUpdatedEvent, RunningPodSpec, TaskId}
import com.mesosphere.usi.core.models.resources.{ResourceRequirement, ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.models.template.{RunTemplate, SimpleRunTemplateFactory}
import com.mesosphere.usi.core.protos.ProtoBuilders.{newAgentId, newDomainInfo, newTaskStatus}
import com.mesosphere.utils.UnitTest
import com.mesosphere.utils.metrics.DummyMetrics
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest.Inside

class MesosEventsLogicTest extends UnitTest with Inside {

  private val mesosEventLogic =
    new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId), MesosMock.masterDomainInfo, DummyMetrics)

  def testRunTemplate(cpus: Int = Integer.MAX_VALUE, mem: Int = 256): RunTemplate = {
    val resourceRequirements = List.newBuilder[ResourceRequirement]
    if (cpus > 0)
      resourceRequirements += ScalarRequirement(ResourceType.CPUS, cpus)
    if (mem > 0)
      resourceRequirements += ScalarRequirement(ResourceType.MEM, mem)
    SimpleRunTemplateFactory(
      resourceRequirements = resourceRequirements.result(),
      shellCommand = "sleep 3600",
      role = "test"
    )
  }

  val testPodId = PodId("mock-podId")
  def podWith1Cpu256Mem: RunningPodSpec =
    RunningPodSpec(testPodId, testRunTemplate(cpus = 1, mem = 256), HomeRegionFilter)

  "MesosEventsLogic" should {
    "decline an offer when there is no PodSpec to launch" in {
      val someOffer = MesosMock.createMockOffer()
      val (_, schedulerEventsBuilder) = mesosEventLogic.matchOffer(
        someOffer,
        Seq()
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual someOffer.getId
    }

    "decline an offer when none of the PodSpec's resource requirements are met" in {
      val insufficientOffer = MesosMock.createMockOffer(cpus = 1)
      val (_, schedulerEventsBuilder) = mesosEventLogic.matchOffer(
        insufficientOffer,
        Seq(RunningPodSpec(testPodId, testRunTemplate(cpus = Integer.MAX_VALUE), HomeRegionFilter))
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual insufficientOffer.getId
    }

    "decline a remote region offer when the default region selector is used" in {
      val remoteOffer =
        MesosMock.createMockOffer(cpus = 10, mem = 1024, domain = newDomainInfo(region = "remote", zone = "a"))
      val runTemplate = testRunTemplate(cpus = 1, mem = 256)
      val defaultRegionPodSpec = RunningPodSpec(testPodId, runTemplate, HomeRegionFilter)
      val remoteRegionPodSpec = RunningPodSpec(testPodId, runTemplate, domainFilter = RegionFilter("remote"))
      inside(mesosEventLogic.matchOffer(remoteOffer, Seq(defaultRegionPodSpec))) {
        case (_, schedulerEventsBuilder) =>
          schedulerEventsBuilder.result.mesosCalls.map(_.hasAccept) shouldBe Seq(false)
      }
      inside(mesosEventLogic.matchOffer(remoteOffer, Seq(remoteRegionPodSpec))) {
        case (_, schedulerEventsBuilder) =>
          schedulerEventsBuilder.result.mesosCalls.map(_.hasAccept) shouldBe Seq(true)
      }
    }

    "accept an offer when some PodSpec's resource requirements are met" in {
      val offerFor4Pods = MesosMock.createMockOffer(cpus = 1 * 10, mem = 256 * 4)
      val (matchedPodIds, schedulerEventsBuilder) = mesosEventLogic.matchOffer(
        offerFor4Pods,
        Seq(
          podWith1Cpu256Mem.copy(id = PodId("podid-1")),
          podWith1Cpu256Mem.copy(id = PodId("podid-2")),
          podWith1Cpu256Mem.copy(id = PodId("podid-3")),
          podWith1Cpu256Mem.copy(id = PodId("podid-4")),
          podWith1Cpu256Mem.copy(id = PodId("podid-5"))
        )
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val accepts = schedulerEventsBuilder.result.mesosCalls.head.getAccept.getOfferIdsList
      accepts.size() shouldBe 1
      accepts.get(0) shouldEqual offerFor4Pods.getId
      matchedPodIds.size shouldEqual 4
    }

    "acknowledge status updates for new pods (first task update)" in {
      import com.mesosphere.usi.core.protos.ProtoBuilders._

      val taskId = newTaskId(podWith1Cpu256Mem.id.value)
      val taskUpdate = newTaskUpdateEvent(taskStatus(taskId))

      val events = mesosEventLogic.processEvent(SchedulerState.empty)(taskUpdate)

      events.mesosCalls.length shouldEqual 1
      events.mesosCalls.head.getAcknowledge.getTaskId shouldEqual taskId

      events.stateEvents.length shouldEqual 1
      events.stateEvents.head shouldBe a[PodStatusUpdatedEvent]
      val podStatusUpdate = events.stateEvents.head.asInstanceOf[PodStatusUpdatedEvent]
      podStatusUpdate.newStatus shouldBe defined
      podStatusUpdate.newStatus.get.taskStatuses should contain(
        TaskId(taskId.getValue) -> taskUpdate.getUpdate.getStatus
      )
    }

    "acknowledge status updates for old pods (new task update)" in {
      import com.mesosphere.usi.core.protos.ProtoBuilders._

      val taskId = newTaskId(podWith1Cpu256Mem.id.value)
      val taskUpdate = newTaskUpdateEvent(taskStatus(taskId))
      val podStatus = Map(
        podWith1Cpu256Mem.id -> PodStatus(
          podWith1Cpu256Mem.id,
          Map(
            TaskId(podWith1Cpu256Mem.id.value) -> taskUpdate.getUpdate.getStatus
          )
        )
      )

      val events = mesosEventLogic.processEvent(
        SchedulerState(Map.empty, podStatus, Map.empty)
      )(taskUpdate)

      events.mesosCalls.length shouldEqual 1
      events.mesosCalls.head.getAcknowledge.getTaskId shouldEqual taskId

      events.stateEvents.length shouldEqual 1
      events.stateEvents.head shouldBe a[PodStatusUpdatedEvent]
      val podStatusUpdate = events.stateEvents.head.asInstanceOf[PodStatusUpdatedEvent]
      podStatusUpdate.newStatus shouldBe defined
      podStatusUpdate.newStatus.get.taskStatuses should contain(
        TaskId(taskId.getValue) -> taskUpdate.getUpdate.getStatus
      )
    }

    "do not ACK status update when no UUID is set" in {
      import com.mesosphere.usi.core.protos.ProtoBuilders._

      val taskId = newTaskId(podWith1Cpu256Mem.id.value)
      val taskUpdateWithoutUuid = newTaskUpdateEvent(taskStatus(taskId, uuid = null))
      val podStatus = Map(
        podWith1Cpu256Mem.id -> PodStatus(
          podWith1Cpu256Mem.id,
          Map(
            TaskId(podWith1Cpu256Mem.id.value) -> taskUpdateWithoutUuid.getUpdate.getStatus
          )
        )
      )

      val events = mesosEventLogic.processEvent(
        SchedulerState(Map.empty, podStatus, Map.empty)
      )(taskUpdateWithoutUuid)

      events.mesosCalls.isEmpty should be(true)
    }
  }

  private def taskStatus(
      taskId: Mesos.TaskID,
      state: Mesos.TaskState = Mesos.TaskState.TASK_RUNNING,
      agentId: Mesos.AgentID = newAgentId("some-agent-id"),
      uuid: ByteString = ByteString.copyFromUtf8("uuid")
  ): Mesos.TaskStatus =
    newTaskStatus(
      taskId = taskId,
      state = state,
      agentId = agentId,
      uuid = uuid
    )

}
