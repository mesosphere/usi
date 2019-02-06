package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.{PodId, PodRecord, PodSpec}
import com.mesosphere.usi.models._

case class Frame(podSpecs: Map[PodId, PodSpec], podRecords: Map[PodId, PodRecord], podStatuses: Map[PodId, PodStatus]) {

  def applyStateEffects(effects: FrameEffects): Frame = {
    var newPodRecords = podRecords
    var newPodStatuses = podStatuses
    effects.stateEvents.foreach {
      case recordChange: PodRecordUpdated =>
        recordChange.newRecord match {
          case Some(newRecord) =>
            newPodRecords = newPodRecords.updated(recordChange.id, newRecord)
          case None =>
            newPodRecords -= recordChange.id
        }
      case statusChange: PodStatusUpdated =>
        statusChange.newStatus match {
          case Some(newStatus) =>
            newPodStatuses = newPodStatuses.updated(statusChange.id, newStatus)
          case None =>
            newPodStatuses -= statusChange.id
        }
      case agentRecordChange: AgentRecordUpdated => // TODO
      case reservationStatusChange: ReservationStatusUpdated => // TODO
      case statusSnapshot: StateSnapshot => // TODO
    }

    copy(podRecords = newPodRecords, podStatuses = newPodStatuses)
  }

}

object Frame {
  val empty = Frame(Map.empty, Map.empty, Map.empty)
}
