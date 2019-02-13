package com.mesosphere.usi.core.models

/**
  * Specification used to launch a [[PodSpec]]
  */
case class RunSpec(resourceRequirements: Seq[ResourceRequirement], commandBuilder: CommandInfoGenerator)
