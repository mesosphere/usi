package com.mesosphere.usi.core.models

import org.apache.mesos.v1.{Protos => Mesos}

trait ResourceRequirement {
  def description: String
  def resourceType: ResourceType
  def matchAndConsume(resource: Seq[Mesos.Resource]): Option[ResourceMatchResult]
}
