package com.mesosphere.usi.core.matching
import com.mesosphere.usi.core.ResourceUtil
import com.mesosphere.usi.core.models.{ResourceMatchResult, ResourceRequirement, ResourceType}

import scala.annotation.tailrec
import org.apache.mesos.v1.{Protos => Mesos}

case class ScalarResourceRequirement(resourceType: ResourceType, amount: Double) extends ResourceRequirement {
  import com.mesosphere.usi.core.ProtoConversions._
  override def description: String = s"${resourceType}:${amount}"
  @tailrec private def iter(
      unmatchedResources: List[Mesos.Resource],
      remainingResources: List[Mesos.Resource]): Option[ResourceMatchResult] = {
    remainingResources match {
      case Nil =>
        None
      case next :: rest =>
        if (next.getScalar.getValue >= amount) {
          Some(
            ResourceMatchResult(
              next.toBuilder.setScalar(amount.asProtoScalar).build :: Nil,
              unmatchedResources ++ rest ++ ResourceUtil.consumeScalarResource(next, amount)))
        } else {
          iter(next :: unmatchedResources, rest)
        }
    }
  }

  override def matchAndConsume(resources: Seq[Mesos.Resource]): Option[ResourceMatchResult] = {
    iter(Nil, resources.toList)
  }
}
