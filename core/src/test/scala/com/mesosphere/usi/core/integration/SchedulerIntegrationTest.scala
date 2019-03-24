package com.mesosphere.usi.core.integration

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.{RangeRequirement, ScalarRequirement}
import com.mesosphere.usi.repository.InMemoryPodRecordRepository
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.scalatest.Inside

class SchedulerIntegrationTest extends AkkaUnitTest with MesosClusterTest with Inside {
  implicit val materializer = ActorMaterializer()

  lazy val settings = MesosClientSettings(mesosFacade.url)
  val frameworkInfo = Protos.FrameworkInfo
    .newBuilder()
    .setUser("test")
    .setName("SimpleHelloWorldExample")
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .build()

  lazy val mesosClient: MesosClient = MesosClient(settings, frameworkInfo).runWith(Sink.head).futureValue
  lazy val schedulerFlow = Scheduler.fromClient(mesosClient, new InMemoryPodRecordRepository)
  lazy val (input, output) = specInputSource(SpecsSnapshot.empty)
    .via(schedulerFlow)
    .toMat(outputFlatteningSink)(Keep.both)
    .run

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "launches the thing" in {
    val podId = PodId("scheduler-integration-test-pod")

    input.offer(
      PodSpecUpdated(
        podId,
        Some(
          PodSpec(
            podId,
            Goal.Running,
            RunSpec(
              resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
              shellCommand = "sleep 3600")
          ))
      ))

    inside(output.pull().futureValue) {
      case Some(snapshot: StateSnapshot) =>
        snapshot shouldBe StateSnapshot.empty
    }
    inside(output.pull().futureValue) {
      case Some(podRecord: PodRecordUpdated) =>
        podRecord.id shouldBe podId
    }
    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdated) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }

  "launches pod with ports" in {
    val podId = PodId("pod-with-ports")

    input.offer(
      PodSpecUpdated(
        podId,
        Some(PodSpec(
          podId,
          Goal.Running,
          RunSpec(
            resourceRequirements =
              List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256), RangeRequirement.ports(Seq(0))),
            shellCommand = "sleep 3600")
        ))
      ))

    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdated) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }
}
