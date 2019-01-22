package com.mesosphere.usi

import com.mesosphere.usi.core.models._

package models {
  case class ReservationSpec(id: String)

  case class ReservationStatus(id: ReservationId, reservationState: ReservationState /* TODO make enum */)

  /**
    * Describes the task statuses of some pod. Note, this is a separate piece of data from a PodRecord. USI manages
    * podRecord for persistent recovery of pod facts, and podStatus is the status as directly reported from Mesos.
    *
    * If we have a podRecord without a podStatus, then this means the task request was launched but we've not heard back
    * yet.
    * If we have a podStatus without a podRecord, then this means we have discovered a spurious pod for which there is
    * no specification.
    */
  case class PodStatus(id: PodId,
                       taskStatuses: Map[String, Mesos.TaskStatus] /* TODO: use Mesos task state */)

  /**
    * Persistent record of agent attributes and other pertinent non-recoverable facts that are inputs for launching pods
    */
  case class AgentRecord(id: AgentId, hostname: String)


  sealed trait ReservationState

  object ReservationState {
    case object NotReserved extends ReservationState
    case object Reserved extends ReservationState
    case object Resizing extends ReservationState
  }

}

package object models {
  /*
   * TODO - we'll want to model these better; Future frameworks migrating to USI are unlikely to have a specific way to
   * format taskIds, so we'll effectively need to accept any mesos-valid taskId. (String)
   */
  type TaskId = String

}
