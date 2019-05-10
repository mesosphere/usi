package com.mesosphere.usi.core.models

/**
  * Sealed trait including all events that can describe the evolution of SchedulerState (statuses and records).
  *
  * These events are emitted by the SchedulerLogic in response to evolution of specification states and Mesos events.
  * They are provided so the framework-implementation can replicate and react to the evolution of that state as best
  * suits the framework's domain.
  */
sealed trait StateEvent
sealed trait PodStateEvent extends StateEvent {
  def id: PodId
}

case class StateSnapshot(podRecords: Seq[PodRecord] = Nil, agentRecords: Seq[AgentRecord] = Nil) extends StateEvent
object StateSnapshot {
  val empty = StateSnapshot()
}

/**
  * Trait which describes an update for any of the USI managed state.
  */
sealed trait StateUpdated extends StateEvent

/**
  * Captures a pod event that was caused by a [[SpecEvent]] submitted by the user.
  */
sealed trait UserError extends PodStateEvent

case class PodInvalid(id: PodId, errors: Seq[String]) extends UserError
case class PodStatusUpdated(id: PodId, newStatus: Option[PodStatus]) extends StateUpdated with PodStateEvent
case class PodRecordUpdated(id: PodId, newRecord: Option[PodRecord]) extends StateUpdated with PodStateEvent
case class AgentRecordUpdated(id: PodId, newRecord: Option[AgentRecord]) extends StateUpdated with PodStateEvent
case class ReservationStatusUpdated(id: ReservationId, newStatus: Option[ReservationStatus]) extends StateUpdated
