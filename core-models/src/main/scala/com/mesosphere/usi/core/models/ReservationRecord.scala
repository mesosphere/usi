package com.mesosphere.usi.core.models

case class ReservationRecord(sequenceNr: Long,
                             reservationId: ReservationId,
                             agentId: AgentId,
                             resources: Resources)