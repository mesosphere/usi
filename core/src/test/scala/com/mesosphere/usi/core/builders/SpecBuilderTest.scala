package com.mesosphere.usi.core.builders

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.logic.MesosEventsLogic
import com.mesosphere.usi.core.models.{FetchUri, PodId, SimpleRunTemplate, RunningPodSpec}
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.{Protos => Mesos}
import com.mesosphere.usi.core.protos.ProtoConversions._
import com.mesosphere.usi.core.protos.ProtoBuilders.{newResource, newResourceAllocationInfo}
import java.net.URI

class SpecBuilderTest extends UnitTest {

  private val mesosEventLogic = new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId))

  "MesosEventsLogic" should {
    "build Mesos proto from RunSpec when fetchUri is defined" in {
      Given("a PodSpec with a RunSpec with fetchUri defined")
      val fetchMe = "http://foo.bar"
      val pod: RunningPodSpec = RunningPodSpec(
        PodId("mock-podId"),
        SimpleRunTemplate(
          resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 32)),
          shellCommand = "sleep 3600",
          role = "test",
          fetch = Seq(FetchUri(uri = URI.create(fetchMe), extract = false))
        )
      )

      Then("taskInfo built from the RunSpec should contain fetch Uri")
      val taskInfo = mesosEventLogic.buildTaskInfos(
        pod,
        MesosMock.mockAgentId.asProto,
        resources = List(
          newResource(
            "cpus",
            Mesos.Value.Type.SCALAR,
            newResourceAllocationInfo("some-role"),
            scalar = 4d.asProtoScalar))
      )

      val uri = taskInfo.head.getCommand.getUris(0)

      uri.getValue shouldBe fetchMe
      uri.getExtract shouldBe false
      uri.getExecutable shouldBe false
      uri.getCache shouldBe false
    }

    "build Mesos proto from RunSpec when dockerImageName is defined" in {
      Given("a PodSpec with a RunSpec with dockerImageName defined")
      val containerName = Option("foo/bar:tag")
      val pod: RunningPodSpec = RunningPodSpec(
        PodId("mock-podId"),
        SimpleRunTemplate(
          resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 32)),
          shellCommand = "sleep 3600",
          role = "test",
          dockerImageName = containerName
        )
      )

      Then("taskInfo built from the RunSpec should contain fetch Uri")
      val taskInfo = mesosEventLogic.buildTaskInfos(
        pod,
        MesosMock.mockAgentId.asProto,
        resources = List(
          newResource(
            "cpus",
            Mesos.Value.Type.SCALAR,
            newResourceAllocationInfo("some-role"),
            scalar = 4d.asProtoScalar))
      )

      val imageName = taskInfo.head.getContainer.getMesos.getImage.getDocker.getName

      imageName shouldBe containerName.get
    }
  }
}
