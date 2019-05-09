package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.{
  PodId,
  PodRecord,
  PodRecordUpdatedEvent,
  PodSpec,
  PodSpecUpdatedEvent,
  PodStatus,
  PodStatusUpdatedEvent,
  StateEventOrSnapshot
}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall}

case class SchedulerEvents(stateEvents: List[StateEventOrSnapshot] = Nil, mesosCalls: List[MesosCall] = Nil)
object SchedulerEvents {
  val empty = SchedulerEvents()
}

/**
  * Immutable intents builder
  *
  * Effects are appended in lazy fashion by using the exposed effect methods.
  *
  * Effects preserve order. For append-efficiency a link-list data structure is used. List is lazily reversed when
  * effects are asked for.
  *
  * Note, SchedulerEventsBuilder will accumulate state events. The scheduler logic loop happens to consume this.
  */
case class SchedulerEventsBuilder(
                                   reverseStateEvents: List[StateEventOrSnapshot] = Nil,
                                   reverseMesosCalls: List[MesosCall] = Nil) {
  lazy val result = SchedulerEvents(reverseStateEvents.reverse, reverseMesosCalls.reverse)

  def ++(other: SchedulerEventsBuilder): SchedulerEventsBuilder = {
    if (other == SchedulerEventsBuilder.empty)
      this
    else if (this == SchedulerEventsBuilder.empty)
      other
    else
      SchedulerEventsBuilder(
        other.reverseStateEvents ++ reverseStateEvents,
        other.reverseMesosCalls ++ reverseMesosCalls)
  }

  /**
    * Append an effect which will change some PodStatus.
    *
    * @param id PodId
    * @param newStatus Optional value; None means remove
    * @return A FrameEffects with this effect appended
    */
  def withPodStatus(id: PodId, newStatus: Option[PodStatus]): SchedulerEventsBuilder =
    withStateEvent(PodStatusUpdatedEvent(id, newStatus))

  /**
    * Append an effect which will change some PodRecord.
    *
    * @param id PodId
    * @return A FrameEffects with this effect appended
    */
  def withPodRecord(id: PodId, newRecord: Option[PodRecord]): SchedulerEventsBuilder =
    withStateEvent(PodRecordUpdatedEvent(id, newRecord))

  def withPodSpec(id: PodId, newPodSpec: Option[PodSpec]): SchedulerEventsBuilder =
    withStateEvent(PodSpecUpdatedEvent(id, newPodSpec))

  def withStateEvent(event: StateEventOrSnapshot): SchedulerEventsBuilder =
    copy(reverseStateEvents = event :: reverseStateEvents)

  /**
    * Append an effect which will cause some Mesos call to be emitted
    *
    * @param call The Mesos call
    * @return A FrameEffects with this effect appended
    */
  def withMesosCall(call: MesosCall): SchedulerEventsBuilder = copy(reverseMesosCalls = call :: reverseMesosCalls)
}

object SchedulerEventsBuilder {
  val empty = SchedulerEventsBuilder()
}
