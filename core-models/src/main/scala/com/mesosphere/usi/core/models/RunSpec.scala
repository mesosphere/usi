package com.mesosphere.usi.core.models
import com.mesosphere.usi.core.launching.CommandBuilder
import com.mesosphere.usi.core.matching.ResourceRequirement

/**
  * Specification used to launch a [[PodSpec]]
  */
case class RunSpec(resourceRequirements: Seq[ResourceRequirement], commandBuilder: CommandBuilder)
