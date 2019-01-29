package com.mesosphere.usi.core.models

/**
  * Snapshot of the reservation state that can't be obtained from mesos.
  * @param reservationId id of the corresponding reservation
  * @param agentId id of the agent
  */
case class ReservationRecord(reservationId: ReservationId, agentId: AgentId)