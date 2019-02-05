package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.mesosphere.usi.models._

case class FrameEffects(reverseStateEvents: List[USIStateEvent] = Nil, reverseMesosCalls: List[Mesos.Call] = Nil) {
  lazy val stateEvents = reverseStateEvents.reverse
  lazy val mesosCalls = reverseMesosCalls.reverse

  def ++(other: FrameEffects): FrameEffects = {
    if (other == FrameEffects.empty)
      this
    else if (this == FrameEffects.empty)
      other
    else
      FrameEffects(other.reverseStateEvents ++ reverseStateEvents, other.reverseMesosCalls ++ reverseMesosCalls)
  }

  def withPodStatus(id: PodId, newStatus: Option[PodStatus]): FrameEffects =
    withStateUpdated(PodStatusUpdated(id, newStatus))
  def withPodRecord(id: PodId, newRecord: Option[PodRecord]): FrameEffects =
    withStateUpdated(PodRecordUpdated(id, newRecord))

  private def withStateUpdated(message: USIStateEvent): FrameEffects = copy(reverseStateEvents = message :: reverseStateEvents)

  def withMesosCall(call: Mesos.Call): FrameEffects = copy(reverseMesosCalls = call :: reverseMesosCalls)
}

object FrameEffects {
  val empty = FrameEffects()
}
