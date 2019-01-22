package com.mesosphere.usi.models

import com.mesosphere.usi.core.models.{PodId, PodRecord, ReservationId}

/**
  * General trait for snapshot or updated events, used to replicate USI's evolving view of the state.
  *
  * These events are emitted by the USI Scheduler Logic in response to evolution of specification states and Mesos
  * events. They are emitted so that a framework can replicate the state and react to changes in a way that best suits
  * the framework's domain.
  */
sealed trait USIStateEvent

case class USIStateSnapshot(podStatuses: Seq[PodStatus], podRecords: Seq[PodRecord], agentRecords: Seq[AgentRecord], reservationStatuses: Seq[ReservationStatus]) extends USIStateEvent

/**
  * Trait which describes an update for any of the USI managed state.
  */
sealed trait USIStateUpdated extends USIStateEvent

case class PodStatusUpdated(id: PodId, newStatus: Option[PodStatus]) extends USIStateUpdated
case class PodRecordUpdated(id: PodId, newRecord: Option[PodRecord]) extends USIStateUpdated
case class AgentRecordUpdated(id: PodId, newRecord: Option[AgentRecord]) extends USIStateUpdated
case class ReservationStatusUpdated(id: ReservationId, newStatus: Option[ReservationStatus]) extends USIStateUpdated
