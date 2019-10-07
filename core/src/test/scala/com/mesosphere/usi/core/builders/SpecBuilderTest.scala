package com.mesosphere.usi.core.builders

import java.net.URI

import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.models.{CurriedPodTaskIdStrategy, FetchUri, PartialTaskId, PodId, RunTemplateLike, SimpleRunTemplate}
import com.mesosphere.usi.core.protos.ProtoConversions._
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.{Protos => Mesos}

class SpecBuilderTest extends UnitTest {

  val podId = PodId("mock-podId")
  import com.mesosphere.usi.core.protos.ProtoBuilders._
  val testOffer = newOffer(
    newOfferId("test"),
    newAgentId("test-agent"),
    MesosMock.mockFrameworkId,
    hostname = "test-host",
    allocationInfo = newResourceAllocationInfo("test"))

  "MesosEventsLogic" should {
    "build Mesos proto from RunSpec when fetchUri is defined" in {
      Given("a PodSpec with a RunSpec with fetchUri defined")
      val fetchMe = "http://foo.bar"
      val runTemplate: RunTemplateLike = SimpleRunTemplate(
        resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 32)),
        shellCommand = "sleep 3600",
        role = "test",
        fetch = Seq(FetchUri(uri = URI.create(fetchMe), extract = false))
      )

      Then("taskInfo built from the RunSpec should contain fetch Uri")
      val Left(operation) = runTemplate.buildOperation(
        testOffer,
        CurriedPodTaskIdStrategy.default(podId),
        Nil,
        Map(
          PartialTaskId.empty ->
            List(
              newResource(
                "cpus",
                Mesos.Value.Type.SCALAR,
                newResourceAllocationInfo("some-role"),
                scalar = 4d.asProtoScalar))))

      val uri = operation.getTaskInfos(0).getCommand.getUris(0)

      uri.getValue shouldBe fetchMe
      uri.getExtract shouldBe false
      uri.getExecutable shouldBe false
      uri.getCache shouldBe false
    }

    "build Mesos proto from RunSpec when dockerImageName is defined" in {
      Given("a PodSpec with a RunSpec with dockerImageName defined")
      val containerName = Option("foo/bar:tag")
      val runTemplate = SimpleRunTemplate(
        resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 32)),
        shellCommand = "sleep 3600",
        role = "test",
        dockerImageName = containerName
      )

      Then("taskInfo built from the RunSpec should contain fetch Uri")
      val Left(operation) = runTemplate.buildOperation(
        testOffer,
        CurriedPodTaskIdStrategy.default(podId),
        Nil,
        Map(
          PartialTaskId.empty ->
            List(
              newResource(
                "cpus",
                Mesos.Value.Type.SCALAR,
                newResourceAllocationInfo("some-role"),
                scalar = 4d.asProtoScalar)))
      )

      val imageName = operation.getTaskInfos(0).getContainer.getMesos.getImage.getDocker.getName

      imageName shouldBe containerName.get
    }
  }
}
