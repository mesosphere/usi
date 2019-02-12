package com.mesosphere.usi.core.matching

import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.tailrec
import com.mesosphere.usi.core.protos.ProtoConversions._
import com.mesosphere.usi.core.models.ResourceType

trait ResourceRequirement {
  def description: String
  def resourceType: ResourceType
  def matchAndConsume(resource: Seq[Mesos.Resource]): Option[ResourceMatchResult]
}

case class ScalarResourceRequirement(resourceType: ResourceType, amount: Double) extends ResourceRequirement {
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
