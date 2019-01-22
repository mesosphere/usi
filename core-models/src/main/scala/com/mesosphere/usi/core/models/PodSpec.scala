package com.mesosphere.usi.core.models

/**
  * Specification of some Pod that should be launched.
  *
  * Pods are launched at-most-once. It is illegal to transition of podSpec from goal terminal to goal running.
  *
  * The deletion of a pod for which a known non-terminal task status exists will result in a spurious pod. Spurious pods
  * can be killed by specifying a podSpec for said spurious pod with goal terminal.
  *
  * @param id Id of the pod
  * @param goal target goal of this pod. See [[Goal]] for more details
  * @param runSpec WIP the thing to run, and resource requirements, etc.
  */
case class PodSpec(id: PodId, goal: Goal, runSpec: RunSpec)
