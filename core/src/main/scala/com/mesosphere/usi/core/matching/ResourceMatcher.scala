package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.models.resources.{RangeRequirement, ResourceRequirement, ScalarRequirement}
import org.apache.mesos.v1.{Protos => Mesos}

object ResourceMatcher {

  /**
    * Logic for matching and consuming resources. Method considers the provided resources, and then matches and consumes
    * the applicable resources
    *
    * @param resources for matching. The Scheduler will only provide the resources matching the ResourceType for this [[ResourceRequirement.
    * @return Some match result if it matches, None if not.
    */
  def matchAndConsume(
      requirement: ResourceRequirement,
      resources: Iterable[Mesos.Resource]
  ): Option[ResourceMatchResult] = {
    requirement match {
      case r: RangeRequirement =>
        RangeResourceMatcher.matchAndConsume(r, resources)
      case s: ScalarRequirement =>
        ScalarResourceMatcher.matchAndConsume(s, resources)
    }
  }
}
