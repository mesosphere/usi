package com.mesosphere.usi.core.models

import org.apache.mesos.v1.{Protos => Mesos}

/**
  * Used for resource matching.
  */
trait ResourceRequirement {
  /**
    * A human readable description for this resource requirement
    */
  def description: String

  /**
    * The ResourceType this requirement describes
    */
  def resourceType: ResourceType

  /**
    * Logic for matching and consuming resources. Method considers the provided resources, and then matches and consumes
    * the applicable resources
    *
    * @param resources for matching. The Scheduler will only provide the resources matching the [[ResourceType]] for this [[ResourceRequirement]].
    * @return Some match result if it matches, None if not.
    */
  def matchAndConsume(resources: Seq[Mesos.Resource]): Option[ResourceMatchResult]
}
