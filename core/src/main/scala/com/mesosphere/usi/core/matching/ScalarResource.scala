package com.mesosphere.usi.core.matching
import com.mesosphere.usi.core.ResourceUtil
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.tailrec

object ScalarResourceMatcher {
  import com.mesosphere.usi.core.protos.ProtoConversions._

  @tailrec private def matchAndConsumeIter(
      scalarRequirement: ScalarRequirement,
      unmatchedResources: List[Mesos.Resource],
      remainingResources: List[Mesos.Resource]): Option[ResourceMatchResult] = {
    remainingResources match {
      case Nil =>
        None
      case next :: rest =>
        if (next.getScalar.getValue >= scalarRequirement.amount) {
          Some(
            ResourceMatchResult(
              next.toBuilder.setScalar(scalarRequirement.amount.asProtoScalar).build :: Nil,
              unmatchedResources ++ rest ++ ResourceUtil.consumeScalarResource(next, scalarRequirement.amount)
            ))
        } else {
          matchAndConsumeIter(scalarRequirement, next :: unmatchedResources, rest)
        }
    }
  }

  def matchAndConsume(
      scalarRequirement: ScalarRequirement,
      resources: Iterable[Mesos.Resource]): Option[ResourceMatchResult] = {
    matchAndConsumeIter(scalarRequirement, Nil, resources.toList)
  }
}
