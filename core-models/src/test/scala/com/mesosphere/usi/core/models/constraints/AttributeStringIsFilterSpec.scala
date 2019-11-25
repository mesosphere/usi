package com.mesosphere.usi.core.models.constraints

import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.Protos

class AttributeStringIsFilterSpec extends UnitTest {
  private def newTextAttribute(name: String, value: String) = {
    Protos.Attribute
      .newBuilder()
      .setName(name)
      .setType(Protos.Value.Type.TEXT)
      .setText(Protos.Value.Text.newBuilder().setValue(value))
      .build
  }
  val offerWithAttributes = Protos.Offer
    .newBuilder()
    .setId(Protos.OfferID.newBuilder().setValue("test-offer"))
    .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("test-framework"))
    .setAgentId(Protos.AgentID.newBuilder().setValue("test-agent"))
    .setHostname("test-hostname")
    .addAttributes(newTextAttribute("rack", "a"))
    .addAttributes(newTextAttribute("profile", "performance"))
    .build

  "it returns false if the provided specified is not defined" in {
    val filter = new AttributeStringIsFilter("cpubrand", "amd")
    filter(offerWithAttributes) shouldBe false
  }

  "it returns true if the provided attribute is defined and the attribute name matches exactly" in {
    val filter = new AttributeStringIsFilter("profile", "performance")
    filter(offerWithAttributes) shouldBe true
  }
}
