package com.mesosphere.usi.core.models

case class ReservationStatus(id: ReservationId, reservationState: ReservationState)

sealed trait ReservationState

object ReservationState {
  case object NotReserved extends ReservationState
  case object Reserved extends ReservationState
  case object Resizing extends ReservationState
}
