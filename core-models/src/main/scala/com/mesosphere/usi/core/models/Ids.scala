package com.mesosphere.usi.core.models

/**
  * Unique identifier of a pod
  * @param value
  */
case class PodId(value: String)

/**
  * Unique identifier of a reservation
  * @param value
  */
case class ReservationId(value: String)

/**
  * Unique identifier of a mesos agent
  * @param value
  */
case class AgentId(value: String)
