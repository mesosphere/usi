package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement

/**
  * Launch template used to launch a pod
  *
  * Support is rather primitive now. We will add richer support for TaskInfo and TaskGroupInfo specification in
  * (DCOS-48503, DCOS-47481)
  *
  * @param resourceRequirements a list of resource requirements for the [[RunningPodSpec]]. See [[ResourceRequirement]]
  *                             class for more information
  * @param shellCommand         tasks' shell command
  * @param fetch                a list of artifact URIs that are passed to Mesos fetcher module and resolved at runtime
  */
case class RunTemplate(
    resourceRequirements: Seq[ResourceRequirement],
    shellCommand: String,
    role: String,
    fetch: Seq[FetchUri] = Seq.empty)
