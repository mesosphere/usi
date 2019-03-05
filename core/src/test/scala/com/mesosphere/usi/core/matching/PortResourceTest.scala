package com.mesosphere.usi.core.matching

import java.util

import com.mesosphere.usi.core.models.ResourceType
import com.mesosphere.usi.core.protos.ProtoBuilders
import com.mesosphere.utils.UnitTestLike
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.Value

import scala.collection.JavaConverters._

class PortResourceTest extends UnitTestLike {
  private val mockRole = "mock-role"
  private val basicAllocationInfo = Protos.Resource.AllocationInfo.newBuilder().setRole(mockRole).build()

  "PortResource" should {
    "select dynamic port from a given range" in {
      val resource = RangeResource.ports(Seq(0, 0))
      val result = resource.matchAndConsume(Seq(resourceWithPortRange(Range(3100, 3200))))

      val matchedResources = result.get.matchedResources
      matchedResources.size should be(1)
      val firstMatched = matchedResources.head
      firstMatched.getRanges.getRangeCount should be(1)
      firstMatched.getRanges
        .getRange(0)
        .getBegin <= 3200 should be(true) withClue "Matched port must be in the given range"
      firstMatched.getRanges
        .getRange(0)
        .getBegin >= 3100 should be(true) withClue "Matched port must be in the given range"
    }

    "produce no match when not given any port range resource" in {
      val resource = RangeResource.ports(Seq(80))
      val result = resource.matchAndConsume(
        Seq(
          ProtoBuilders.newResource(
            ResourceType.CPUS.name,
            Protos.Value.Type.SCALAR,
            basicAllocationInfo,
            scalar = Protos.Value.Scalar.newBuilder().setValue(1D).build
          )))

      result.isDefined should be(false) withClue "Expecting no match on resource that does not contain ports"
    }

    "get ports from multiple ranges" in {
      val resource = RangeResource.ports(Seq(0, 0, 0, 0, 0))
      val result = resource.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2002), Range(3100, 3200))))

      val matchedResources = result.get.matchedResources
      matchedResources.size should be(1)
    }

    "get non-dynamic ports from multiple ranges" in {
      val resource = RangeResource.ports(Seq(80, 81, 82, 83, 100))
      val result = resource.matchAndConsume(Seq(resourceWithPortRange(Range(80, 83), Range(100, 100))))

      val matchedResources = result.get.matchedResources
      matchedResources.size should be(1)
      val firstMatched = matchedResources.head
      firstMatched.getRanges.getRangeCount should be(2)
      rangesEqual(firstMatched.getRanges, Range(80, 83), Range(100, 100)) should be(true) withClue "Expecting ranges to be matching the ranges of expected ports"
    }

    "produce no match when more ports requested than available" in {
      val resource = RangeResource.ports(Seq(0, 0, 0, 0, 0))
      val result = resource.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2002))))

      result.isDefined should be(false)
    }

    "produce no match if required ports are not available" in {
      val resource = RangeResource.ports(Seq(80))
      val result = resource.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2002))))

      result.isDefined should be(false)
    }

    "select the ports in random ranges" in {
      val resource1 = RangeResource.ports(Seq(0), new util.Random(0))
      val match1 = resource1.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2400))))
      val resource2SameSeed = RangeResource.ports(Seq(0), new util.Random(0))
      val match2 = resource2SameSeed.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2400))))

      match1.get.matchedResources.head.getRanges.getRange(0) should be(
        match2.get.matchedResources.head.getRanges.getRange(0))

      val differentMatch = (1 to 100).find { seed =>
        val resource2DifferentSeed = RangeResource.ports(Seq(0), new util.Random(seed))
        val match3 = resource2DifferentSeed.matchAndConsume(Seq(resourceWithPortRange(Range(2000, 2400))))
        match1.get.matchedResources.head.getRanges.getRange(0).getBegin == match3.get.matchedResources.head.getRanges
          .getRange(0)
          .getBegin && match1.get.matchedResources.head.getRanges
          .getRange(0)
          .getEnd == match3.get.matchedResources.head.getRanges.getRange(0).getEnd
      }

      differentMatch.isDefined should be(true) withClue "Expecting ports to be selected at random"
    }
  }

  private def rangesEqual(resultRanges: Value.Ranges, expecting: Range*): Boolean = {
    val resultRangesAsScala = resultRanges.getRangeList.asScala
    expecting.forall(expextedRange =>
      resultRangesAsScala.exists(r => r.getBegin == expextedRange.start && r.getEnd == expextedRange.end))
  }

  private def resourceWithPortRange(ranges: Range*): Protos.Resource = {

    val protoRanges: Seq[Protos.Value.Range] = ranges.map(
      r =>
        Protos.Value.Range.newBuilder
          .setBegin(r.start)
          .setEnd(r.end)
          .build)

    val rangesProto = Protos.Value.Ranges.newBuilder
      .addAllRange(protoRanges.asJava)
      .build

    ProtoBuilders.newResource(
      ResourceType.PORTS.name,
      Protos.Value.Type.RANGES,
      basicAllocationInfo,
      ranges = rangesProto)
  }

}
