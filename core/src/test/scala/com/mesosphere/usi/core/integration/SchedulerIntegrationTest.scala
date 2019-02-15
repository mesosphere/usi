package com.mesosphere.usi.core.integration

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.matching.ScalarResourceRequirement
import com.mesosphere.usi.core.models._
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import org.apache.mesos.v1.Protos
import org.scalatest.Inside

class SchedulerIntegrationTest extends AkkaUnitTest with MesosClusterTest with Inside {
  "launches the thing" in {
    implicit val materializer = ActorMaterializer()

    val settings = MesosClientSettings(mesosFacade.url)
    val frameworkInfo = Protos.FrameworkInfo.newBuilder().setUser("test").setName("SimpleHelloWorldExample").build()

    val client = Scheduler.connect(settings, frameworkInfo).futureValue

    val podId = PodId("pod-1")
    val (input, output) = specInputSource(SpecsSnapshot.empty)
      .via(client.schedulerFlow)
      .toMat(outputFlatteningSink)(Keep.both)
      .run

    input.offer(PodSpecUpdated(podId, Some(PodSpec(podId, Goal.Running, RunSpec(
      resourceRequirements = List(
        ScalarResourceRequirement(ResourceType.CPUS, 1),
        ScalarResourceRequirement(ResourceType.MEM, 256)),
      shellCommand = "sleep 3600")))))

    inside(output.pull().futureValue) {
      case Some(snapshot: StateSnapshot) =>
        snapshot shouldBe StateSnapshot.empty
    }
    inside(output.pull().futureValue) {
      case Some(podRecord: PodRecordUpdated) =>
        podRecord.id shouldBe podId
        println(s"agentId = ${podRecord.newRecord.get.agentId}")
    }
    inside(output.pull().futureValue) {
      case Some(podStatusChange: PodStatusUpdated) =>
        podStatusChange.newStatus.get.taskStatuses(TaskId(podId.value)).getState shouldBe Protos.TaskState.TASK_RUNNING
    }
  }
}
