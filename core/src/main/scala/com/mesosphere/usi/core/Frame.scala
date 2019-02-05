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
      case statusSnapshot: USIStateSnapshot => // TODO
    }

    copy(podRecords = newPodRecords, podStatuses = newPodStatuses)
  }

  def applySpecEvent(specEvent: SpecEvent): (Frame, Set[PodId]) = {
    // TODO - assert valid transition
    specEvent match {
      case SpecsSnapshot(podSpecSnapshot, reservationSpecSnapshot) =>
        if (reservationSpecSnapshot.nonEmpty) {
          // This should make the framework crash
          throw new NotImplementedError("ReservationSpec support not yet implemented")
        }
        val newPodsSpecs: Map[PodId, PodSpec] = podSpecSnapshot.map { pod =>
          pod.id -> pod
        }(collection.breakOut)

        val changedPodIds = podSpecs.keySet ++ newPodsSpecs.keySet
        (copy(podSpecs = newPodsSpecs), changedPodIds)

      case PodSpecUpdated(id, newState) =>
        val newPodSpecs = newState match {
          case Some(podSpec) =>
            podSpecs.updated(id, podSpec)
          case None =>
            podSpecs - id
        }
        (copy(podSpecs = newPodSpecs)) -> Set(id)

      case ReservationSpecUpdated(id, _) =>
        throw new NotImplementedError("ReservationSpec support not yet implemented")
    }
  }
}
