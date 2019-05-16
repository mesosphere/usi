package com.mesosphere.usi.core.integration

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.commandInputSource
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.{RangeRequirement, ScalarRequirement}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.scalatest.Inside

class SchedulerIntegrationTest extends AkkaUnitTest with MesosClusterTest with Inside {
  implicit val materializer = ActorMaterializer()

  lazy val settings = MesosClientSettings.load().withMasters(Seq(mesosFacade.url))
  val frameworkInfo = Protos.FrameworkInfo
    .newBuilder()
    .setUser("test")
    .setName("SimpleHelloWorldExample")
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .build()

  lazy val mesosClient: MesosClient = MesosClient(settings, frameworkInfo).runWith(Sink.head).futureValue
  lazy val (snapshot, schedulerFlow) = Scheduler.fromClient(mesosClient, InMemoryPodRecordRepository()).futureValue
  lazy val (input, output) = commandInputSource
    .via(schedulerFlow)
    .toMat(Sink.queue())(Keep.both)
    .run

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "launches a pod with Mesos and reports the status" in {
    val podId = PodId("scheduler-integration-test-pod")

    input.offer(
      LaunchPod(
        podId,
        RunSpec(
          resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
          shellCommand = "sleep 3600",
          "test")
      ))

    inside(output.pull().futureValue) {
      case Some(specUpdated: PodSpecUpdatedEvent) =>
        specUpdated.id shouldBe podId
    }
    inside(output.pull().futureValue) {
      case Some(recordUpdated: PodRecordUpdatedEvent) =>
        recordUpdated.id shouldBe podId
    }
    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdatedEvent) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }

  "launches pod with ports" in {
    val podId = PodId("pod-with-ports")

    input.offer(
      LaunchPod(
        podId,
        RunSpec(
          resourceRequirements =
            List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256), RangeRequirement.ports(Seq(0))),
          shellCommand = "sleep 3600",
          "test"
        )
      ))

    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdatedEvent) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }
}
