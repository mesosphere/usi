package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.models.{ResourceMatchResult, ResourceType}
import com.mesosphere.usi.core.protos.{ProtoBuilders, ProtoConversions}
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest.Inside

class ScalarResourceRequirementTest extends UnitTest with Inside {

  import ProtoConversions._
  "ScalarResourceRequirement" should {
    "match and consume a cpu resource" in {
      val matchers = ScalarResource.cpus(2.0)

      val resources =
        List(
          ProtoBuilders.newResource(
            ResourceType.CPUS.name,
            Mesos.Value.Type.SCALAR,
            ProtoBuilders.newResourceAllocationInfo("some-role"),
            scalar = 10.asProtoScalar))
      inside(matchers.matchAndConsume(resources)) {
        case Some(ResourceMatchResult(List(matchedResult), List(unmatched))) =>
          matchedResult.getScalar.getValue shouldBe 2.0
          unmatched.getScalar.getValue shouldBe 8.0
      }
    }
  }
}
