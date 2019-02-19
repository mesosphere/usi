package com.mesosphere.usi.core.models

/**
  * Specification used to launch a [[PodSpec]]
  *
  * We currently only support a shellCommand. This is primitive. We will add richer support for TaskInfo and
  * TaskGroupInfo specification in (DCOS-48503, DCOS-47481)
  */
case class RunSpec(resourceRequirements: Seq[ResourceRequirement], shellCommand: String)
