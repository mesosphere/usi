package com.mesosphere.usi.core.integration

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.matching.ScalarResource
import com.mesosphere.usi.core.models._
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.scalatest.Inside

class SchedulerIntegrationTest extends AkkaUnitTest with MesosClusterTest with Inside {
  "launches the thing" in {
    implicit val materializer = ActorMaterializer()

    val settings = MesosClientSettings(mesosFacade.url)
    val frameworkInfo = Protos.FrameworkInfo
      .newBuilder()
      .setUser("test")
      .setName("SimpleHelloWorldExample")
      .addRoles("test")
      .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
      .build()

    val client = MesosClient(settings, frameworkInfo).runWith(Sink.head).futureValue
    val schedulerFlow = Scheduler.fromClient(client)

    val podId = PodId("scheduler-integration-test-pod")
    val (input, output) = specInputSource(SpecsSnapshot.empty)
      .via(schedulerFlow)
      .toMat(outputFlatteningSink)(Keep.both)
      .run

    input.offer(
      PodSpecUpdated(
        podId,
        Some(
          PodSpec(
            podId,
            Goal.Running,
            RunSpec(
              resourceRequirements = List(ScalarResource.cpus(1), ScalarResource.memory(256)),
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
        println(s"agentId = ${podRecord.newRecord.get.agentId}")
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
}
