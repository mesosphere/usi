package com.mesosphere.usi.helloworld.runspecs

import java.time.Instant
import java.util.UUID

import com.google.protobuf.ByteString
import com.mesosphere.usi.core.models.{
  AgentId,
  PodId,
  PodRecord,
  PodRecordUpdatedEvent,
  PodSpecUpdatedEvent,
  PodStatus,
  PodStatusUpdatedEvent,
  TaskId,
  TerminalPodSpec
}
import com.mesosphere.usi.helloworld.runspecs.InstanceStatus.{
  RunningInstance,
  SentToMesos,
  StagingInstance,
  TerminalInstance,
  UnreachableInstance
}
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.{Protos => Mesos}

class InstanceTrackerSpec extends UnitTest {

  "InMemoryInstanceTracker" should {
    "create a new instance state when handling PodRecordUpdatedEvent" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.size shouldEqual 1
    }
    "ignore all other events if the instance state doesn't exist" in new Fixture {

      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.size shouldEqual 0
    }
    "ignore PodRecordUpdatedEvent if the instance state already exist" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case _ => fail("PodRecordUpdatedEvent wasn't ignored")
      }
    }
    "make a transition from SentToMesos state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "make a transition from SentToMesos state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "make a transition from SentToMesos state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from SentToMesos state to Unreachable" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(unreachableStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[SentToMesos] =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Staging state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "make a transition from Staging state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "make a transition from Staging state to Unreachable" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[StagingInstance] =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "ignore a transition from Staging state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "ignore a transition from Staging state to SentToMesos" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Running state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "make a transition from Running state to Unreachable" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[RunningInstance] =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "ignore a transition from Running state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "ignore a transition from Running state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "ignore a transition from Running state to SentToMesos" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "ignore all transitions from Terminal state" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalStateUpdate())

      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())

      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "ignore a transition from Unreachable state to Unreachable" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[SentToMesos] =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(SentToMesos) state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(SentToMesos) state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(SentToMesos) state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(Staging) state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(Staging) state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(Staging) state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: TerminalInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "ignore a transition from Unreachable(Staging) state to SentToMesos" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[StagingInstance] =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(Running) state to Running" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: RunningInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "make a transition from Unreachable(Running) state to Terminal" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: TerminalInstance =>
        case other => fail(s"Excepted Terminal but found $other")
      }
    }

    "ignore a transition from Unreachable(Running) state to Staging" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(stagingStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[RunningInstance] =>
        case other => fail(s"Excepted UnreachableInstance but found $other")
      }
    }

    "ignore a transition from Unreachable(Running) state to SentToMesos" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(unreachableStateUpdate())
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case s: UnreachableInstance if s.lastSeenInstanceState.isInstanceOf[RunningInstance] =>
        case other => fail(s"Excepted UnreachableInstance but found $other")
      }
    }

    "pick a new incarnation id if detected by the event" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      tracker.processUpdate(stagingStateUpdate(podId = serviceSpecInstanceId.nextIncarnation.toPodId))

      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }
    "Reject events from old instance incarnations" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalStateUpdate())
      val newIncarnationPodId = serviceSpecInstanceId.nextIncarnation.toPodId
      tracker.processUpdate(stagingStateUpdate(podId = newIncarnationPodId))
      tracker.processUpdate(runningStateUpdate())

      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.status match {
        case _: StagingInstance =>
        case other => fail(s"Excepted staging but found $other")
      }
    }

    "Mark instance as terminating when TerminatingPodSpec found" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalPodSpecUpdatedEvent)

      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.head._2.isTerminating shouldEqual true
    }
    "Remove instance from state if it terminates while marked isTerminating" in new Fixture {
      tracker.processUpdate(podRecordUpdatedEvent)
      tracker.processUpdate(stagingStateUpdate())
      tracker.processUpdate(runningStateUpdate())
      tracker.processUpdate(terminalPodSpecUpdatedEvent)
      tracker.processUpdate(terminalStateUpdate())
      tracker.serviceState(serviceSpecInstanceId.serviceSpecId).get.instances.size shouldEqual 0
    }

  }
}

class Fixture {
  val tracker = new InMemoryInstanceTracker

  val uuid = UUID.randomUUID()
  val podID = PodId(s"hello.${uuid.toString}.1")
  val serviceSpecInstanceId = ServiceSpecInstanceId.fromPodId(podID)

  import com.mesosphere.usi.core.protos.ProtoBuilders._

  private def taskStatus(
      taskId: Mesos.TaskID = newTaskId("hello-mesos-task"),
      state: Mesos.TaskState,
      agentId: Mesos.AgentID = newAgentId("some-agent-id"),
      uuid: ByteString = ByteString.copyFromUtf8("uuid")
  ): Mesos.TaskStatus = newTaskStatus(
    taskId = taskId,
    state = state,
    agentId = agentId,
    uuid = uuid
  )

  def stagingStateUpdate(podId: PodId = podID) = {
    PodStatusUpdatedEvent(
      podId,
      Some(PodStatus(podId, Map(TaskId("taskId") -> taskStatus(state = Mesos.TaskState.TASK_STAGING)))))
  }

  def runningStateUpdate(podId: PodId = podID) = {
    PodStatusUpdatedEvent(
      podId,
      Some(PodStatus(podId, Map(TaskId("taskId") -> taskStatus(state = Mesos.TaskState.TASK_RUNNING)))))
  }

  def terminalStateUpdate(podId: PodId = podID) = {
    PodStatusUpdatedEvent(
      podId,
      Some(PodStatus(podId, Map(TaskId("taskId") -> taskStatus(state = Mesos.TaskState.TASK_FINISHED)))))
  }

  def unreachableStateUpdate(podId: PodId = podID) = {
    PodStatusUpdatedEvent(
      podId,
      Some(PodStatus(podId, Map(TaskId("taskId") -> taskStatus(state = Mesos.TaskState.TASK_UNREACHABLE)))))
  }

  def podRecordUpdatedEvent = {
    PodRecordUpdatedEvent(podID, Some(PodRecord(podID, Instant.now(), AgentId("agent"))))
  }

  def terminalPodSpecUpdatedEvent = {
    PodSpecUpdatedEvent(podID, Some(TerminalPodSpec(podID)))
  }

}
