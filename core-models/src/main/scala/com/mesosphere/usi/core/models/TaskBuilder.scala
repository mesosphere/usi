package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import org.apache.mesos.v1.{Protos => Mesos}

trait TaskBuilder {
  def resourceRequirements: Seq[ResourceRequirement]
  def buildTask(
      matchedOffer: Mesos.Offer,
      taskResources: Seq[Mesos.Resource],
      peerTaskResources: Map[PartialTaskId, Seq[Mesos.Resource]]): Mesos.TaskInfo.Builder
}
