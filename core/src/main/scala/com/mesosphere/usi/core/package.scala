package com.mesosphere.usi

package object core {
  /*
   * TODO - we'll want to model these better; Future frameworks migrating to USI are unlikely to have a specific way to
   * format taskIds, so we'll effectively need to accept any mesos-valid taskId. (String)
   */
  type TaskId = String
  type PodId = String
  type ReservationId = String

  case class PodSpec(id: PodId, goal: String, runSpec: String)

  case class ReservationSpec(id: String)

  case class SpecSnapshot(podSpecs: Seq[PodSpec], reservationSpecs: Seq[ReservationSpec])


  case class StatusSnapshot(podStatuses: Seq[PodStatus], reservationStatuses: Seq[ReservationStatus])


  sealed trait MesosTaskStatus
  object MesosTaskStatus {
    case object TASK_RUNNING extends MesosTaskStatus
  }

  /**
    * Describes the status of some pod
    */
  case class PodStatus(id: String,
                       taskStatuses: Map[String, MesosTaskStatus] /* TODO: use Mesos task state */ )


  sealed trait ReservationState
  object ReservationState {
    case object NotReserved extends ReservationState
    case object Reserved extends ReservationState
    case object Resizing extends ReservationState
  }

  case class ReservationStatus(id: ReservationId, reservationState: ReservationState /* TODO make enum */)

  // Status is a bad name
  sealed trait StatusUpdate
  case class PodStatusUpdate(id: PodId, newStatus: Option[PodStatus]) extends StatusUpdate
  case class ReservationStatusUpdate(id: ReservationId, newStatus: Option[ReservationStatus]) extends StatusUpdate

  sealed trait SpecUpdate
  case class PodSpecUpdate(id: PodId, newState: Option[PodSpec]) extends SpecUpdate
  case class ReservationSpecUpdate(id: ReservationId, newState: Option[ReservationSpec]) extends SpecUpdate

}

