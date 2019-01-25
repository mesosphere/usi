package com.mesosphere.usi.core.models

/**
  * * ReservationRecord is an immutable event of created reservations .
  * @param sequenceNr number used for ordering the reservation record events.
  * @param reservationId id of the reservation
  * @param agentId id of the agent
  * @param resources reserved resources
  */
case class ReservationRecord(sequenceNr: Long,
                             reservationId: ReservationId,
                             agentId: AgentId,
                             resources: Resources)