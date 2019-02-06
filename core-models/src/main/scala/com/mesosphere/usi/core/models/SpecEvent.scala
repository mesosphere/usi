package com.mesosphere.usi.core.models

sealed trait SpecEvent

/**
  * Used to describe the initial state on which all SpecUpdated events will apply
  * @param podSpecs
  * @param reservationSpecs
  */
case class SpecsSnapshot(podSpecs: Seq[PodSpec], reservationSpecs: Seq[ReservationSpec]) extends SpecEvent

/**
  * Used to communicate that a specification was updated (added, changed, or deleted)
  */
sealed trait SpecUpdated extends SpecEvent

case class PodSpecUpdated(id: PodId, newState: Option[PodSpec]) extends SpecUpdated

case class ReservationSpecUpdated(id: ReservationId, newState: Option[ReservationSpec]) extends SpecUpdated
