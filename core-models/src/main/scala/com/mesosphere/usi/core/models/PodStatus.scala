package com.mesosphere.usi.core.models

/**
  * Describes the task statuses of some pod. Note, this is a separate piece of data from a PodRecord. USI manages
  * podRecord for persistent recovery of pod facts, and podStatus is the status as directly reported from Mesos.
  *
  * If we have a podRecord without a podStatus, then this means the task request was launched but we've not heard back
  * yet.
  * If we have a podStatus without a podRecord, then this means we have discovered a spurious pod for which there is
  * no specification.
  */
case class PodStatus(id: PodId, taskStatuses: Map[TaskId, Mesos.TaskStatus] /* TODO: use Mesos task state */ )
