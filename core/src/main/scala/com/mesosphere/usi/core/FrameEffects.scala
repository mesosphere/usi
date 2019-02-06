package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.{
  Mesos,
  PodId,
  PodRecord,
  PodRecordUpdated,
  PodStatus,
  PodStatusUpdated,
  StateEvent
}

/**
  * Immutable effects builder
  *
  * Effects are appended in lazy fashion by using the exposed effect methods.
  *
  * Effects preserve order. For append-efficiency a link-list data structure is used. List is lazily reversed when
  * effects are asked for.
  *
  * @param reverseStateEvents
  * @param reverseMesosCalls
  */
case class FrameEffects(reverseStateEvents: List[StateEvent] = Nil, reverseMesosCalls: List[Mesos.Call] = Nil) {
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

  /**
    * Append an effect which will change some PodStatus.
    *
    * @param id PodId
    * @param newStatus Optional value; None means remove
    * @return A FrameEffects with this effect appended
    */
  def withPodStatus(id: PodId, newStatus: Option[PodStatus]): FrameEffects =
    withStateUpdated(PodStatusUpdated(id, newStatus))

  /**
    * Append an effect which will change some PodRecord.
    *
    * @param id PodId
    * @param newStatus Optional value; None means remove
    * @return A FrameEffects with this effect appended
    */
  def withPodRecord(id: PodId, newRecord: Option[PodRecord]): FrameEffects =
    withStateUpdated(PodRecordUpdated(id, newRecord))

  private def withStateUpdated(message: StateEvent): FrameEffects =
    copy(reverseStateEvents = message :: reverseStateEvents)

  /**
    * Append an effect which will cause some Mesos call to be emitted
    *
    * @param call The Mesos call
    * @return A FrameEffects with this effect appended
    */
  def withMesosCall(call: Mesos.Call): FrameEffects = copy(reverseMesosCalls = call :: reverseMesosCalls)
}

object FrameEffects {
  val empty = FrameEffects()
}
