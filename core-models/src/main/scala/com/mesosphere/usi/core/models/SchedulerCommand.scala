package com.mesosphere.usi.core.models

/**
  * Trait which includes all possible events that can describe the evolution of the framework implementation's
  * Specification state.
  *
  * Supported commands:
  * - [[LaunchPod]]
  * - [[KillPod]]
  * - [[ExpungePod]]
  * - [[CreateReservation]]
  */
sealed trait SchedulerCommand

/**
  * Launch the specified pod at-most-once. Frameworks *SHOULD NOT* reuse podIds; if restarting a pod, kill the old and
  * launch a new with a different id.
  *
  * Submitting a pod to the scheduler for launching will result the appropriate offer revive call. Once an offer is
  * received with suitable resources for the pod, a record of the matching pod is created, persisted, prior to the pod
  * being launched.
  *
  * Submitting a launch command for a pod that is already launched (has record of being launched), or is pending launch,
  * is a no-op.
  *
  * @param podId
  * @param runSpec
  */
case class LaunchPod(podId: PodId, runSpec: RunSpec) extends SchedulerCommand

/**
  * Send a kill for tasks associated with the specified podId.
  *
  * If no task status is known for the specified podId, then the kill is a no-op.
  * @param podId
  */
case class KillPod(podId: PodId) extends SchedulerCommand

/**
  * Delete the pod record and any pending pod spec (running or kill) for a given pod. Does not result in kill
  * (immediately, nor immediately).
  *
  * The associated PodStatus, if it exists, will remain until the pod is either terminal or unreachable.
  *
  * @param podId
  */
case class ExpungePod(podId: PodId) extends SchedulerCommand

case class CreateReservation(id: ReservationId, newState: Option[ReservationSpec]) extends SchedulerCommand
