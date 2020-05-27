package com.mesosphere.usi.core.models

/**
  * Unique identifier of a pod
  *
  * @param value
  */
case class PodId(value: String) {

  val validNameRegex = "^[a-zA-Z0-9\\-\\.]+$"

  require(value.matches(validNameRegex), s"Pod id $value must only contain alphanum, . or -.")
  require(value.nonEmpty, s"Pod id must not be empty")
}

/**
  * Name for a pod's task. The TaskName and PodId are combined via PodTaskIdStrategy to generate a TaskId.
  *
  * @param value
  */
case class TaskName(value: String)

object TaskName {
  val empty = TaskName("")
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
