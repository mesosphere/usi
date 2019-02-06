package com.mesosphere.usi.core.models

/**
  * Specification used to launch a [[PodSpec]]
  * @param cpus number of cpus requested by the pod
  */
case class RunSpec(cpus: Double)
