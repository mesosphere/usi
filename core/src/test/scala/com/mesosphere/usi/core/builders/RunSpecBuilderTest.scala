package com.mesosphere.usi.core.builders

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.logic.MesosEventsLogic
import com.mesosphere.usi.core.models.{FetchUri, Goal, PodId, PodSpec, RunSpec}
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.utils.UnitTest

import java.net.URI

class RunSpecBuilderTest extends UnitTest {

  private val mesosEventLogic = new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId))

  // Ideally, we wouldn't test the whole schedulerFlow but rather only the part that builds protobuf out of RunSpecs
  "MesosEventsLogic" should {
    "build Mesos proto from RunSpec when fetchUri is defined" in {
      Given("a PodSpec with a RunSpec with fetchUri defined")
      val fetchMe = "http://foo.bar"
      val pod: PodSpec = PodSpec(
        PodId("mock-podId"),
        Goal.Running,
        RunSpec(
          resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 1), ScalarRequirement(ResourceType.MEM, 32)),
          shellCommand = "sleep 3600",
          fetch = Seq(FetchUri(uri = URI.create(fetchMe), extract = false))
        )
      )

      And("an big enough offer")
      val offer = MesosMock.createMockOffer()

      Then("scheduler should accept the offer")
      // This right here should test only converting RunSpec to protobuf - not the offer matching logic
      val (matchedPodIds, schedulerEventsBuilder) = mesosEventLogic.matchOffer(offer, Seq(pod))
      matchedPodIds.size shouldEqual 1
      logger.info(s"matchedPodIds.size = ${matchedPodIds.size}")

      And("Mesos calls should contain an ACCEPT call with the fetch Uri")
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val operation = schedulerEventsBuilder.result.mesosCalls.head.getAccept.getOperations(0)
      val uri = operation.getLaunch.getTaskInfos(0).getCommand.getUris(0)

      uri.getValue shouldBe fetchMe
      uri.getExtract shouldBe false
      uri.getExecutable shouldBe false
      uri.getCache shouldBe false
    }
  }
}
