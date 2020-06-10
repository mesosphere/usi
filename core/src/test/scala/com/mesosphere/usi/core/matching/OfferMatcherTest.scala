package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.constraints.AttributeStringIsFilter
import com.mesosphere.usi.core.models.{PodId, RunningPodSpec}
import com.mesosphere.usi.core.models.faultdomain.HomeRegionFilter
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.models.template.{RunTemplate, SimpleRunTemplateFactory}
import com.mesosphere.usi.core.protos.ProtoBuilders.newTextAttribute
import com.mesosphere.utils.UnitTest

class OfferMatcherTest extends UnitTest {
  val agentAttributes = List(newTextAttribute("rack", "a"), newTextAttribute("class", "baremetal"))
  val offer = MesosMock.createMockOffer(cpus = 4, mem = 4096, attributes = agentAttributes)
  val testPodId = PodId("mock-podId")
  def testRunTemplate(cpus: Int = Integer.MAX_VALUE, mem: Int = 256): RunTemplate = {
    SimpleRunTemplateFactory(
      resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, cpus), ScalarRequirement(ResourceType.MEM, mem)),
      shellCommand = "sleep 3600",
      "test"
    )
  }
  val offerMatcher = new OfferMatcher(MesosMock.masterDomainInfo)

  "agent attribute matching" should {
    "decline offers which don't match all of the attribute filters" in {
      val nonMatchingPodSpec = RunningPodSpec(
        testPodId,
        testRunTemplate(cpus = 1, mem = 256),
        HomeRegionFilter,
        List(AttributeStringIsFilter("rack", "a"), AttributeStringIsFilter("class", "vm"))
      )

      val result = offerMatcher.matchOffer(offer, List(nonMatchingPodSpec))
      result shouldBe Map.empty
    }

    "accept offers when all of the attribute filters match" in {
      val nonMatchingPodSpec = RunningPodSpec(
        testPodId,
        testRunTemplate(cpus = 1, mem = 256),
        HomeRegionFilter,
        List(AttributeStringIsFilter("rack", "a"), AttributeStringIsFilter("class", "baremetal"))
      )

      val result = offerMatcher.matchOffer(offer, List(nonMatchingPodSpec))
      result.nonEmpty shouldBe true
    }

    "accept offers when no attribute filters are specified" in {
      val nonMatchingPodSpec = RunningPodSpec(testPodId, testRunTemplate(cpus = 1, mem = 256), HomeRegionFilter, Nil)

      val result = offerMatcher.matchOffer(offer, List(nonMatchingPodSpec))
      result.nonEmpty shouldBe true
    }
  }
}
