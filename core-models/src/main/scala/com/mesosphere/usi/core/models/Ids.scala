package com.mesosphere.usi.core.models

/**
  * Unique identifier of a pod
  * @param value
  */
case class PodId(value: String)

/**
  * Identifier for a pod's task; does not include podId
  * @param value
  */
case class PartialTaskId(value: String)

object PartialTaskId {
  val empty = PartialTaskId("")
}

/**
  * Unique identifier of a reservation
  * @param value
  */
case class ReservationId(value: String)

/**
  * Unique identifier of a Mesos agent
  * @param value
  */
case class AgentId(value: String)

/**
  * Unique Mesos task identifier
  * @param value
  */
case class TaskId(value: String)
