package com.mesosphere.usi.core.models

import org.apache.mesos.v1.{Protos => Mesos}

/**
  * Describes the task statuses of some pod. Note, this is a separate piece of data from a [[PodRecord]]. USI manages
  * [[PodRecord]] for persistent recovery of pod facts, and [[PodStatus]] is the status as directly reported from Mesos.
  *
  * If we have a [[PodRecord]] without a [[PodStatus]], then this means the task request was launched but we've not
  * heard back yet.
  *
  * If we have a [[PodStatus]] without a [[PodRecord]], then this means we have discovered a spurious pod for which
  * there is no specification.
  */
case class PodStatus(id: PodId, taskStatuses: Map[TaskId, Mesos.TaskStatus] /* TODO: use Mesos task state */ )
