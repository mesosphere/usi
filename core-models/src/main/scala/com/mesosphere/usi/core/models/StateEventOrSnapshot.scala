package com.mesosphere.usi.core.models

/**
  * Sealed trait including all events that can describe the evolution of SchedulerState (statuses and records).
  *
  * These events are emitted by the SchedulerLogic in response to evolution of specification states and Mesos events.
  * They are provided so the framework-implementation can replicate and react to the evolution of that state as best
  * suits the framework's domain.
  *
  * The result of all StateEvent
  */
sealed trait StateEventOrSnapshot

/**
  * Trait which describes an update for any of the USI managed state.
  *
  * All of these events, excepting [[PodInvalid]], are emitted in the same order they were processed by USI so that the
  * implementation framework can replicate the state of USI, so that it can have a local (to the modulue), read-only,
  * version of state indexed as the implementation framework sees fit.
  *
  * Usually, the orchestrator will respond to [[PodRecord]] events.
  */
sealed trait StateEvent extends StateEventOrSnapshot

/**
  * Implemented by all traits whose updates describe evolution to persisted state
  */
sealed trait PersistedStateUpdatedEvent extends StateEvent

sealed trait PodStateEvent extends StateEventOrSnapshot {
  def id: PodId
}

case class StateSnapshot(podRecords: Seq[PodRecord], agentRecords: Seq[AgentRecord]) extends StateEventOrSnapshot
object StateSnapshot {
  def empty = StateSnapshot(Nil, Nil)
}

/**
  * Captures a pod event that was caused by a [[SchedulerCommand]] submitted by the user.
  */
sealed trait UserError extends PodStateEvent

/**
  * The RunSpec submitted from a [[LaunchPod]] command had obvious errors that would prevent it from launching. If the
  * implementation framework receive these, then it should be considered a bug.
  *
  * @param id The podId for which the errors were detected
  * @param errors Text description of the errors
  */
case class PodInvalid(id: PodId, errors: Seq[String]) extends UserError

/**
  * A Pod is specified to be launched, or killed; or, the pod met the objective.
  *
  * If [[PodSpec]] is [[RunningPodSpec]], then the pod is launching and offer matching is occurring.
  * If [[PodSpec]] is [[TerminalPodSpec]], then USI is killing a pod and has not yet received a terminal status for the
  * pod.
  *
  * A PodSpec is cleared, then there is no pending action for the podSpec.
  * @param id
  * @param newState
  */
case class PodSpecUpdatedEvent(id: PodId, newState: Option[PodSpec]) extends StateEvent with PodStateEvent

/** Sent each time a Mesos task status is received for one of the tasks in a pod.
  */
case class PodStatusUpdatedEvent(id: PodId, newStatus: Option[PodStatus]) extends StateEvent with PodStateEvent

/**
  * Sent each time a [[PodRecord]] for some pod has changed.
  *
  * Ideally, orchestration logic primarily reacts on changes to this state, as this state is deterministic and can
  * always be reliably recovered after a crash.
  * @param id
  * @param newRecord
  */
case class PodRecordUpdatedEvent(id: PodId, newRecord: Option[PodRecord])
    extends PersistedStateUpdatedEvent
    with PodStateEvent
case class AgentRecordUpdatedEvent(id: PodId, newRecord: Option[AgentRecord])
    extends PersistedStateUpdatedEvent
    with PodStateEvent
