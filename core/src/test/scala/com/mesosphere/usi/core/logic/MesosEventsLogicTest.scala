package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.matching.ScalarResource
import com.mesosphere.usi.core.models.Goal
import com.mesosphere.usi.core.models.PodId
import com.mesosphere.usi.core.models.PodSpec
import com.mesosphere.usi.core.models.ResourceType
import com.mesosphere.usi.core.models.RunSpec
import com.mesosphere.utils.UnitTest

class MesosEventsLogicTest extends UnitTest {

  private val mesosEventLogic = new MesosEventsLogic(new MesosCalls(MesosMock.mockFrameworkId))

  val mockPodSpecWith1Cpu256Mem: PodSpec = PodSpec(
    PodId("mock-podId"),
    Goal.Running,
    RunSpec(
      List(ScalarResource(ResourceType.CPUS, 1), ScalarResource(ResourceType.MEM, 256)),
      shellCommand = "sleep 3600")
  )

  "MesosEventsLogic" should {
    "decline an offer when there is no PodSpec to launch" in {
      val someOffer = MesosMock.createMockOffer()
      val (_, schedulerEventsBuilder) = mesosEventLogic matchOffer (
        someOffer,
        Seq()
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual someOffer.getId
    }

    "decline an offer when none of the PodSpec's resource requirements are met" in {
      val insufficientOffer = MesosMock.createMockOffer(cpus = 1)
      val (_, schedulerEventsBuilder) = mesosEventLogic matchOffer (
        insufficientOffer,
        Seq(
          mockPodSpecWith1Cpu256Mem.copy(
            runSpec = mockPodSpecWith1Cpu256Mem.runSpec.copy(
              resourceRequirements = Seq(ScalarResource(ResourceType.CPUS, Integer.MAX_VALUE))
            )))
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val declines = schedulerEventsBuilder.result.mesosCalls.head.getDecline.getOfferIdsList
      declines.size() shouldBe 1
      declines.get(0) shouldEqual insufficientOffer.getId
    }

    "accept an offer when some PodSpec's resource requirements are met" in {
      val offerFor4Pods = MesosMock.createMockOffer(cpus = 1 * 10, mem = 256 * 4)
      val (matchedPodIds, schedulerEventsBuilder) = mesosEventLogic matchOffer (
        offerFor4Pods,
        Seq(
          mockPodSpecWith1Cpu256Mem.copy(id = PodId("podid-1")),
          mockPodSpecWith1Cpu256Mem.copy(id = PodId("podid-2")),
          mockPodSpecWith1Cpu256Mem.copy(id = PodId("podid-3")),
          mockPodSpecWith1Cpu256Mem.copy(id = PodId("podid-4")),
          mockPodSpecWith1Cpu256Mem.copy(id = PodId("podid-5")),
        )
      )
      schedulerEventsBuilder.result.mesosCalls.size shouldBe 1
      val accepts = schedulerEventsBuilder.result.mesosCalls.head.getAccept.getOfferIdsList
      accepts.size() shouldBe 1
      accepts.get(0) shouldEqual offerFor4Pods.getId
      matchedPodIds.size shouldEqual 4
    }
  }

}
