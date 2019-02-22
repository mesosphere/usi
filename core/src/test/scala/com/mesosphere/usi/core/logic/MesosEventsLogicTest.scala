package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.FrameworkMock
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.matching.ScalarResourceRequirement
import com.mesosphere.usi.core.models.PodId
import com.mesosphere.usi.core.models.ResourceType
import com.mesosphere.utils.UnitTest

class MesosEventsLogicTest extends UnitTest {

  val mesosEventLogic = new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId))

  "MesosEventsLogic" should {
    "decline an offer when PodSpec's are empty" in {
      val (_, schedulerEventsBuilder) = mesosEventLogic matchOffer(
        MesosMock.mockOffer,
        Seq()
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual MesosMock.mockOffer.getId
    }

    "decline an offer when PodSpec's resource requirements are unmet" in {
      val largeRunSpec = FrameworkMock.mockRunSpec.copy(
        resourceRequirements = Seq(ScalarResourceRequirement(ResourceType.CPUS, Integer.MAX_VALUE))
      )
      val (_, schedulerEventsBuilder) = mesosEventLogic matchOffer(
        MesosMock.mockOffer,
        Seq(FrameworkMock.mockPodSpec.copy(runSpec = largeRunSpec))
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual MesosMock.mockOffer.getId
    }

    "accept an offer when PodSpec's resource requirements are met" in {
      val (matchedPodIds, schedulerEventsBuilder) = mesosEventLogic matchOffer(
        MesosMock.mockOffer,
        Seq(
          FrameworkMock.mockPodSpec.copy(id = PodId("podid-1")),
          FrameworkMock.mockPodSpec.copy(id = PodId("podid-2")),
          FrameworkMock.mockPodSpec.copy(id = PodId("podid-3")),
          FrameworkMock.mockPodSpec.copy(id = PodId("podid-4")),
          FrameworkMock.mockPodSpec.copy(id = PodId("podid-5")),
        )
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val accepts = schedulerEventsBuilder.result.mesosCalls.head.getAccept.getOfferIdsList
      accepts.size() shouldBe 1
      accepts.get(0) shouldEqual MesosMock.mockOffer.getId
      matchedPodIds.size shouldEqual 4
    }
  }

}
