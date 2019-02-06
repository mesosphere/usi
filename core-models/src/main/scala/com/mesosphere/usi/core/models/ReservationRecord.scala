package com.mesosphere.usi.core.models

/**
  * Snapshot of reservation state; persists details that can't be reliably obtained from Mesos during Framework
  * recovery.
  *
  * @param reservationId id of the corresponding reservation
  * @param agentId id of the agent
  */
case class ReservationRecord(reservationId: ReservationId, agentId: AgentId)
