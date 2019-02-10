package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.{
  PodId,
  PodRecord,
  PodRecordUpdated,
  PodStatus,
  PodStatusUpdated,
  StateEvent
}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall}


case class SchedulerLogicIntents(stateIntents: List[StateEvent] = Nil, mesosIntents: List[MesosCall] = Nil)
object SchedulerLogicIntents {
  val empty = SchedulerLogicIntents()
}

/**
  * Immutable intents builder
  *
  * Effects are appended in lazy fashion by using the exposed effect methods.
  *
  * Effects preserve order. For append-efficiency a link-list data structure is used. List is lazily reversed when
  * effects are asked for.
  *
  * @param reverseStateEvents
  * @param reverseMesosCalls
  */
case class SchedulerLogicIntentsBuilder(reverseStateEvents: List[StateEvent] = Nil, reverseMesosCalls: List[MesosCall] = Nil) {
  lazy val result = SchedulerLogicIntents(reverseStateEvents.reverse, reverseMesosCalls.reverse)

  def ++(other: SchedulerLogicIntentsBuilder): SchedulerLogicIntentsBuilder = {
    if (other == SchedulerLogicIntentsBuilder.empty)
      this
    else if (this == SchedulerLogicIntentsBuilder.empty)
      other
    else
      SchedulerLogicIntentsBuilder(other.reverseStateEvents ++ reverseStateEvents, other.reverseMesosCalls ++ reverseMesosCalls)
  }

  /**
    * Append an effect which will change some PodStatus.
    *
    * @param id PodId
    * @param newStatus Optional value; None means remove
    * @return A FrameEffects with this effect appended
    */
  def withPodStatus(id: PodId, newStatus: Option[PodStatus]): SchedulerLogicIntentsBuilder =
    withStateUpdated(PodStatusUpdated(id, newStatus))

  /**
    * Append an effect which will change some PodRecord.
    *
    * @param id PodId
    * @param newStatus Optional value; None means remove
    * @return A FrameEffects with this effect appended
    */
  def withPodRecord(id: PodId, newRecord: Option[PodRecord]): SchedulerLogicIntentsBuilder =
    withStateUpdated(PodRecordUpdated(id, newRecord))

  private def withStateUpdated(message: StateEvent): SchedulerLogicIntentsBuilder =
    copy(reverseStateEvents = message :: reverseStateEvents)

  /**
    * Append an effect which will cause some Mesos call to be emitted
    *
    * @param call The Mesos call
    * @return A FrameEffects with this effect appended
    */
  def withMesosCall(call: MesosCall): SchedulerLogicIntentsBuilder = copy(reverseMesosCalls = call :: reverseMesosCalls)
}

object SchedulerLogicIntentsBuilder {
  val empty = SchedulerLogicIntentsBuilder()
}
