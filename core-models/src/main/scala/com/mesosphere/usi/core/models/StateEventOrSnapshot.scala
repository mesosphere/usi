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

sealed trait PodStateEvent extends StateEvent {
  def id: PodId
}

case class StateSnapshot(podRecords: Seq[PodRecord], agentRecords: Seq[AgentRecord]) extends StateEventOrSnapshot
object StateSnapshot {
  def empty = StateSnapshot(Nil, Nil)
}

/**
  * These specs are managed by the Scheduler in response to [[LaunchPod]] and [[KillPod]] commands. They indicate that
  * there is some pending action to perform on behalf of a pod.
  *
  * The podSpec is cleared from USI under the following conditions:
  *
  * jf [[PodSpec]] is [[RunningPodSpec]], then the pod is launching and offer matching is occurring.
  * If [[PodSpec]] is [[TerminalPodSpec]], then USI is killing a pod and has not yet received a terminal status for the
  * pod.
  * @param id
  * @param newState the possible new state. None indicates a removal.
  */
case class PodSpecUpdatedEvent(id: PodId, newState: Option[PodSpec]) extends StateEvent with PodStateEvent

object PodSpecUpdatedEvent {
  def forUpdate(podSpec: PodSpec) = PodSpecUpdatedEvent(podSpec.id, Some(podSpec))
}

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
