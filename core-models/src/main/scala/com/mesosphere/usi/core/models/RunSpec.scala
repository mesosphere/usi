package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement

/**
  * Specification used to launch a [[PodSpec]]
  *
  * We currently only support a shellCommand. This is primitive. We will add richer support for TaskInfo and
  * TaskGroupInfo specification in (DCOS-48503, DCOS-47481)
  *
  * @param resourceRequirements a list of resource requirements for the [[PodSpec]]. See [[ResourceRequirement]]
  *                             class for more information
  * @param shellCommand tasks' shell command
  * @param fetch a list of artifact URIs that are passed to Mesos fetcher module and resolved at runtime
  */
case class RunSpec(
    resourceRequirements: Seq[ResourceRequirement],
    shellCommand: String,
    fetch: Seq[FetchUri] = Seq.empty)
