package com.mesosphere.usi

import java.time.{Instant, ZonedDateTime}

package object models {
  /*
   * TODO - we'll want to model these better; Future frameworks migrating to USI are unlikely to have a specific way to
   * format taskIds, so we'll effectively need to accept any mesos-valid taskId. (String)
   */
  type TaskId = String
  type PodId = String
  type ReservationId = String

  sealed trait Goal
  object Goal {
    case object Running extends Goal
    case object Terminal extends Goal
  }

  case class PodSpec(id: PodId, goal: Goal, runSpec: String)
  case class PodRecord(id: PodId, launchedAt: Instant)
  case class ReservationSpec(id: String)

  sealed trait StatusMessage
  case class StatusSnapshot(podStatuses: Seq[PodStatus], reservationStatuses: Seq[ReservationStatus]) extends StatusMessage
  sealed trait StatusChange extends StatusMessage
  case class PodStatusChange(id: PodId, newStatus: Option[PodStatus]) extends StatusChange
  case class ReservationStatusChange(id: ReservationId, newStatus: Option[ReservationStatus]) extends StatusChange

  /**
    * Mock Mesos calls / state
    */
  object Mesos {
    sealed trait Call

    object Call {
      case object Revive extends Call
      case object Suppress extends Call
      case class Accept(offerId: String, operations: Seq[Operation]) extends Call
      case class Kill(taskId: String) extends Call
    }


    // stub class that just launches as
    case class Operation(launch: Launch)

    case class Launch(taskInfo: TaskInfo)

    case class TaskInfo(taskId: String)

    case class TaskState(taskId: String, status: TaskStatus)
    sealed trait TaskStatus
    object TaskStatus {
      case object TASK_RUNNING extends TaskStatus
      case object TASK_KILLED extends TaskStatus
    }


    sealed trait Event

    object Event {
      case class Offer(offerId: String) extends Event
      case class Update(status: TaskState) extends Event

    }
  }


  /**
    * Describes the status of some pod
    */
  case class PodStatus(id: String,
                       taskStatuses: Map[String, Mesos.TaskStatus] /* TODO: use Mesos task state */ )


  sealed trait ReservationState
  object ReservationState {
    case object NotReserved extends ReservationState
    case object Reserved extends ReservationState
    case object Resizing extends ReservationState
  }

  case class ReservationStatus(id: ReservationId, reservationState: ReservationState /* TODO make enum */)

  sealed trait SpecMessage
  case class SpecsSnapshot(podSpecs: Seq[PodSpec], reservationSpecs: Seq[ReservationSpec]) extends SpecMessage

  sealed trait SpecChange extends SpecMessage
  case class PodSpecChange(id: PodId, newState: Option[PodSpec]) extends SpecChange
  case class ReservationSpecChange(id: ReservationId, newState: Option[ReservationSpec]) extends SpecChange
}
