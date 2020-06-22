package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import org.apache.mesos.v1.{Protos => Mesos}

/**
  * This trait is implemented to define the logic of how a Mesos TaskInfo is built given some matched offer and
  * resources.
  */
trait TaskBuilder {

  /**
    * The resources (cpus, memory, ports, volumes, etc.) required for the specific task
    * @return
    */
  def resourceRequirements: Seq[ResourceRequirement]

  /**
    * Invoked with the matched offer and resources in order to get the Mesos TaskInfo for the task to be launched.
    *
    * It is illegal to set the following TaskInfo fields, as these are handled by USI:
    *
    * - id
    * - name
    * - resources
    * - agentId
    *
    * @param matchedOffer The offer which was matched and is being used to launch this task
    * @param taskResources The resources specifically matched for this task
    * @param peerTaskResources The resources for all other tasks
    * @return A Mesos v1 TaskInfo builder
    */
  def buildTask(
      builder: Mesos.TaskInfo.Builder,
      matchedOffer: Mesos.Offer,
      taskResources: Seq[Mesos.Resource],
      peerTaskResources: Map[TaskName, Seq[Mesos.Resource]]
  ): Unit
}
