package com.mesosphere.usi.core.logic

import com.google.protobuf.ByteString
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.{SchedulerState, SpecState}
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.{Goal, PodId, PodSpec, PodStatus, PodStatusUpdated, RunSpec, TaskId}
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.protos.ProtoBuilders.{newAgentId, newTaskStatus}
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.{Protos => Mesos}

class MesosEventsLogicTest extends UnitTest {

  private val mesosEventLogic = new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId))

  def podWith1Cpu256Mem(id: String = "mock-podId"): PodSpec =
    PodSpec(
      PodId(id),
      Goal.Running,
      RunSpec(
        Seq(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 256)),
        shellCommand = "sleep 3600")
    ).get

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
        Seq(
          PodSpec(
            PodId("mock-podId"),
            Goal.Running,
            RunSpec(Seq(ScalarRequirement(ResourceType.CPUS, Integer.MAX_VALUE)), shellCommand = "sleep 3600")
          ).get
        )
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual insufficientOffer.getId
    }

    "accept an offer when some PodSpec's resource requirements are met" in {
      val offerFor4Pods = MesosMock.createMockOffer(cpus = 1 * 10, mem = 256 * 4)
      val (matchedPodIds, schedulerEventsBuilder) = mesosEventLogic.matchOffer(
        offerFor4Pods,
        Seq(
          podWith1Cpu256Mem("podid-1"),
          podWith1Cpu256Mem("podid-2"),
          podWith1Cpu256Mem("podid-3"),
          podWith1Cpu256Mem("podid-4"),
          podWith1Cpu256Mem("podid-5"),
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

      val pod = podWith1Cpu256Mem()
      val taskId = newTaskId(pod.id.value)
      val taskUpdate = newTaskUpdateEvent(taskStatus(taskId))
      val specs = SpecState(Map(pod.id -> pod))

      val events = mesosEventLogic.processEvent(specs, SchedulerState.empty, Set.empty)(taskUpdate)

      events.mesosCalls.length shouldEqual 1
      events.mesosCalls.head.getAcknowledge.getTaskId shouldEqual taskId

      events.stateEvents.length shouldEqual 1
      events.stateEvents.head shouldBe a[PodStatusUpdated]
      val podStatusUpdate = events.stateEvents.head.asInstanceOf[PodStatusUpdated]
      podStatusUpdate.newStatus shouldBe defined
      podStatusUpdate.newStatus.get.taskStatuses should contain(
        TaskId(taskId.getValue) -> taskUpdate.getUpdate.getStatus
      )
    }

    "acknowledge status updates for old pods (new task update)" in {
      import com.mesosphere.usi.core.protos.ProtoBuilders._

      val pod = podWith1Cpu256Mem()
      val taskId = newTaskId(pod.id.value)
      val taskUpdate = newTaskUpdateEvent(taskStatus(taskId))
      val specs = SpecState(Map(pod.id -> pod))
      val podStatus = Map(
        pod.id -> PodStatus(
          pod.id,
          Map(
            TaskId(pod.id.value) -> taskUpdate.getUpdate.getStatus
          )))

      val events = mesosEventLogic.processEvent(
        specs,
        SchedulerState(Map.empty, podStatus),
        Set.empty
      )(taskUpdate)

      events.mesosCalls.length shouldEqual 1
      events.mesosCalls.head.getAcknowledge.getTaskId shouldEqual taskId

      events.stateEvents.length shouldEqual 1
      events.stateEvents.head shouldBe a[PodStatusUpdated]
      val podStatusUpdate = events.stateEvents.head.asInstanceOf[PodStatusUpdated]
      podStatusUpdate.newStatus shouldBe defined
      podStatusUpdate.newStatus.get.taskStatuses should contain(
        TaskId(taskId.getValue) -> taskUpdate.getUpdate.getStatus
      )
    }

    "do not ACK status update when no UUID is set" in {
      import com.mesosphere.usi.core.protos.ProtoBuilders._

      val pod = podWith1Cpu256Mem()
      val taskId = newTaskId(pod.id.value)
      val taskUpdateWithoutUuid = newTaskUpdateEvent(taskStatus(taskId, uuid = null))
      val specs = SpecState(Map(pod.id -> pod))
      val podStatus = Map(
        pod.id -> PodStatus(
          pod.id,
          Map(
            TaskId(pod.id.value) -> taskUpdateWithoutUuid.getUpdate.getStatus
          )))

      val events = mesosEventLogic.processEvent(
        specs,
        SchedulerState(Map.empty, podStatus),
        Set.empty
      )(taskUpdateWithoutUuid)

      events.mesosCalls.isEmpty should be(true)
    }
  }

  private def taskStatus(
      taskId: Mesos.TaskID,
      state: Mesos.TaskState = Mesos.TaskState.TASK_RUNNING,
      agentId: Mesos.AgentID = newAgentId("some-agent-id"),
      uuid: ByteString = ByteString.copyFromUtf8("uuid")
  ): Mesos.TaskStatus = newTaskStatus(
    taskId = taskId,
    state = state,
    agentId = agentId,
    uuid = uuid
  )

}
