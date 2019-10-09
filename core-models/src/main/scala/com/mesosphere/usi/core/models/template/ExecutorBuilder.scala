package com.mesosphere.usi.core.models.template

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import org.apache.mesos.v1.{Protos => Mesos}

trait ExecutorBuilder {
  def resourceRequirements: Seq[ResourceRequirement]
  def buildExecutor(matchedOffer: Mesos.Offer, executorResources: Seq[Mesos.Resource]): Mesos.ExecutorInfo.Builder
}
