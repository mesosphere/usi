package com.mesosphere.usi.core

import com.google.protobuf.ByteString
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.core.models.{
  LaunchPod,
  PodId,
  PodRecordUpdatedEvent,
  PodSpecUpdatedEvent,
  PodStatusUpdatedEvent,
  RunTemplate,
  StateEventOrSnapshot,
  StateSnapshot
}
import com.mesosphere.usi.core.protos.ProtoBuilders
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest.Inside

class SchedulerLogicHandlerTest extends UnitTest with Inside {
  val testRoleRunSpec = RunTemplate(Seq.empty, "", "test-role")

  def declineCallsIn(calls: Seq[Call]): Seq[Call.Decline] = calls.collect {
    case call if call.hasDecline => call.getDecline
  }

  def acceptCallsIn(calls: Seq[Call]): Seq[Call.Accept] = calls.collect {
    case call if call.hasAccept => call.getAccept
  }

  def podRecordUpdatesIn(events: Seq[StateEventOrSnapshot]): Seq[PodRecordUpdatedEvent] = events.collect {
    case podRecordUpdated: PodRecordUpdatedEvent => podRecordUpdated
  }

  def podStatusUpdatesIn(events: Seq[StateEventOrSnapshot]): Seq[PodStatusUpdatedEvent] = events.collect {
    case podStatusUpdated: PodStatusUpdatedEvent => podStatusUpdated
  }

  def podSpecUpdatesIn(events: Seq[StateEventOrSnapshot]): Seq[PodSpecUpdatedEvent] = events.collect {
    case podSpecUpdated: PodSpecUpdatedEvent => podSpecUpdated
  }

  "launching pods" should {
    import ProtoBuilders._
    val podId = PodId("running-pod-on-a-mocked-mesos")
    val offer = MesosMock.createMockOffer()
    val taskStatusUUID = ByteString.copyFromUtf8("DEADBEEF")
    val runningTaskStatus =
      newTaskStatus(newTaskId(podId.value), Mesos.TaskState.TASK_RUNNING, newAgentId("testing"), uuid = taskStatusUUID)
    val launchPod = LaunchPod(
      podId,
      RunTemplate(
        resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
        shellCommand = "sleep 3600",
        "test")
    )

    "ignore launch commands for podIds that already have a podRecord" in {
      Given("the scheduler logic already has launched a pod")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)
      handler.handleCommand(launchPod)

      inside(handler.handleMesosEvent(newOfferEvent(offer))) {
        case SchedulerEvents(events, mesosCalls) =>
          acceptCallsIn(mesosCalls).shouldNot(be(empty))
          podRecordUpdatesIn(events).shouldNot(be(empty))
      }

      When("the launch command is sent again")
      val launchResult = handler.handleCommand(launchPod)

      Then("the command is ignored")
      podSpecUpdatesIn(launchResult.stateEvents).shouldBe(empty)

      When("an offer is resubmitted")
      val reofferResult = handler.handleMesosEvent(newOfferEvent(offer))
      acceptCallsIn(reofferResult.mesosCalls).shouldBe(empty)
      declineCallsIn(reofferResult.mesosCalls).shouldNot(be(empty))
    }

    "match a valid offer and reports a running task" in {
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)
      handler.handleCommand(launchPod)

      inside(handler.handleMesosEvent(newOfferEvent(offer))) {
        case SchedulerEvents(stateEvents, mesosCalls) =>
          val Seq(PodRecordUpdatedEvent(_, Some(podRecord))) = podRecordUpdatesIn(stateEvents)
          podRecord.podId shouldBe podId
          podRecord.agentId shouldBe MesosMock.mockAgentId

          val Seq(accept) = acceptCallsIn(mesosCalls)
          accept.getOfferIds(0).shouldBe(offer.getId)
      }

      inside(handler.handleMesosEvent(newTaskUpdateEvent(runningTaskStatus))) {
        case SchedulerEvents(stateEvents, mesosCalls) =>
          val Some(podStatus) = stateEvents.collectFirst { case PodStatusUpdatedEvent(_, Some(podStatus)) => podStatus }
          podStatus.id shouldBe podId

          mesosCalls.head.hasAcknowledge shouldBe true
      }
    }
  }

  "pod status reporting" should {
    import ProtoBuilders._
    val podId = PodId("pod")
    val taskStatusUUID = ByteString.copyFromUtf8("DEADBEEF")
    val runningTaskStatus =
      newTaskStatus(newTaskId(podId.value), Mesos.TaskState.TASK_RUNNING, newAgentId("testing"), uuid = taskStatusUUID)

    "emits a spurious podStatus and acknowledges a task status when receiving a task status for a record-less pod" in {
      Given("Scheduler logic handler with empty state")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)

      When("a task status update is received")
      val result = handler.handleMesosEvent(newTaskUpdateEvent(runningTaskStatus))

      Then("a podStatus should be emitted")
      inside(result.stateEvents.collectFirst { case PodStatusUpdatedEvent(_, Some(podStatus)) => podStatus }) {
        case Some(podStatus) =>
          podStatus.id shouldBe podId
          podStatus.taskStatuses.size shouldBe 1

          val taskStatus = podStatus.taskStatuses.values.head
          taskStatus.getAgentId shouldBe newAgentId("testing")
      } withClue ("Expected a podStatus update, but didn't find one")

      And("the task status should be acknowledged")
      inside(result.mesosCalls.collectFirst { case call if call.hasAcknowledge => call.getAcknowledge }) {
        case Some(acknowledge) =>
          acknowledge.getUuid shouldBe taskStatusUUID
      }
    }

    "removes spurious podStatuses when they become terminal" in {
      Given("Scheduler logic handler with a spurious podStatus state")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)

      When("a spurious task status update is received")
      val resultForRunningTaskStatus = handler.handleMesosEvent(newTaskUpdateEvent(runningTaskStatus))
      resultForRunningTaskStatus.mesosCalls shouldNot be(empty)
      resultForRunningTaskStatus.stateEvents shouldNot be(empty)

      And("the task status turns back to terminal")
      val terminalTaskStatus = runningTaskStatus.toBuilder.setState(Mesos.TaskState.TASK_FINISHED).build()
      val resultForTerminalTaskStatus = handler.handleMesosEvent(newTaskUpdateEvent(terminalTaskStatus))

      Then("the podStatus should be cleared")
      inside(resultForTerminalTaskStatus.stateEvents.collect { case p: PodStatusUpdatedEvent => p }) {
        case Seq(terminalUpdate, pruneUpdate) =>
          terminalUpdate.id shouldBe podId
          terminalUpdate.newStatus shouldNot be(empty)

          pruneUpdate.id shouldBe podId
          pruneUpdate.newStatus shouldBe None
      } withClue ("Expected a podStatus update, but didn't find one")

      And("the task status should be acknowledged")
      inside(resultForTerminalTaskStatus.mesosCalls.collectFirst {
        case call if call.hasAcknowledge => call.getAcknowledge
      }) {
        case Some(acknowledge) =>
          acknowledge.getUuid shouldBe taskStatusUUID
      }
    }

    "removes TerminalPodSpecs when they are reported terminal" in {
      Given("Scheduler logic handler with a spurious podStatus state")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)
      handler.handleMesosEvent(newTaskUpdateEvent(runningTaskStatus))

      And("the task status turns back to terminal")
      val terminalTaskStatus = runningTaskStatus.toBuilder.setState(Mesos.TaskState.TASK_FINISHED).build()
      val resultForTerminalTaskStatus = handler.handleMesosEvent(newTaskUpdateEvent(terminalTaskStatus))

      Then("the podStatus should be cleared")
      inside(resultForTerminalTaskStatus.stateEvents.collect { case p: PodStatusUpdatedEvent => p }) {
        case Seq(terminalUpdate, pruneUpdate) =>
          terminalUpdate.id shouldBe podId
          terminalUpdate.newStatus shouldNot be(empty)

          pruneUpdate.id shouldBe podId
          pruneUpdate.newStatus shouldBe None
      } withClue ("Expected a podStatus update, but didn't find one")

      And("the task status should be acknowledged")
      inside(resultForTerminalTaskStatus.mesosCalls.collectFirst {
        case call if call.hasAcknowledge => call.getAcknowledge
      }) {
        case Some(acknowledge) =>
          acknowledge.getUuid shouldBe taskStatusUUID
      }
    }
  }

  "suppress and revive logic" should {
    "produces a Mesos revive call for a newly launched podSpec's role" in {
      Given("Scheduler logic handler with empty state")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)
      val podId = PodId("pod")

      When("pod with role 'test-role' is launched")
      val result = handler.handleCommand(LaunchPod(podId, testRoleRunSpec))

      Then("revive call is generated for that role")
      inside(result.mesosCalls.collectFirst { case c if c.hasRevive => c.getRevive }) {
        case Some(revive) =>
          revive.getRoles(0).shouldBe("test-role")
      } withClue s"Expecting revive call with role 'test-role' but got ${result.mesosCalls}"

      And("Another revive is generated for the same role when another podSpec is updated")
      val podId2 = PodId("pod2")
      val result2 = handler.handleCommand(LaunchPod(podId2, testRoleRunSpec))
      inside(result2.mesosCalls.collectFirst { case c if c.hasRevive => c.getRevive }) {
        case Some(revive) =>
          revive.getRoles(0).shouldBe("test-role")
      } withClue s"Expecting revive call with role 'test-role' but got ${result.mesosCalls}"
    }

    "produces a Mesos suppress call when all podSpecs for a given role are launched" in {
      Given("Scheduler logic handler with not launched pod")
      val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId), StateSnapshot.empty)
      val podId = PodId("pod")
      // creates not launched pod in the internal state of scheduler logic
      handler.handleCommand(LaunchPod(podId, testRoleRunSpec))

      When("that pod is launched")
      val offer = MesosMock.createMockOffer()
      val result = handler.handleMesosEvent(ProtoBuilders.newOfferEvent(offer))

      Then("suppress call is generated for that role")
      inside(result.mesosCalls.collectFirst { case c if c.hasSuppress => c.getSuppress }) {
        case Some(suppress) =>
          suppress.getRoles(0) == "test-role"
      } withClue s"Expecting suppress call with role 'test-role' but got ${result.mesosCalls}"
    }
  }
}
