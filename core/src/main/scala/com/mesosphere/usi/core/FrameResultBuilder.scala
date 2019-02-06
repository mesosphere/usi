package com.mesosphere.usi.core
import com.mesosphere.usi.core.models._

case class FrameResult(stateEvents: List[StateEvent], mesosIntents: List[Mesos.Call])

/**
  * Immutable helper data structure which:
  *
  * - applies state events as intents to the frame, and accumulates
  * - accumulates mesos call intents
  * - keeps track of which podIds have changed state (so that caches and other logic can react and be re-computed)
  *
  * This data structure enables various stages of the transformation of a frame to be expressed via a pipeline of chained
  * calls
  *
  * @param state The current SchedulerLogic state
  * @param appliedStateEvents The cumulative stateEvents applied to the frame; will be included in the FrameResult
  * @param mesosIntents The cumulative Mesos calls that are intended to be sent
  * @param dirtyPodIds The podIds that have been changed during the lifecycle of this FrameWithEvents instance
  */
case class FrameResultBuilder(state: SchedulerLogicState, appliedStateEvents: List[StateEvent], mesosIntents: List[Mesos.Call], dirtyPodIds: Set[PodId]) {
  private def applyAndAccumulate(intents: SchedulerLogicIntents): FrameResultBuilder = {
    // TODO - we need to handle status snapshots and create a mechanism to signal that all cache should be recomputed
    val newDirty = dirtyPodIds ++ intents.stateIntents.iterator.collect {
      case podEvent: PodStateEvent => podEvent.id
    }
    copy(
      state = state.applyStateIntents(intents.stateIntents),
      dirtyPodIds = newDirty,
      appliedStateEvents = appliedStateEvents ++ intents.stateIntents,
      mesosIntents = mesosIntents ++ intents.mesosIntents)
  }

  /**
    * Applies the specEvent to the frame, marking podIds as dirty accordingly
    *
    * @param specEvent
    */
  def applySpecEvent(specEvent: SpecEvent): FrameResultBuilder = {
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

        val changedPodIds = state.podSpecs.keySet ++ newPodsSpecs.keySet

        copy(state = state.copy(podSpecs = newPodsSpecs), dirtyPodIds = dirtyPodIds ++ changedPodIds)

      case PodSpecUpdated(id, newState) =>
        val newPodSpecs = newState match {
          case Some(podSpec) =>
            state.podSpecs.updated(id, podSpec)
          case None =>
            state.podSpecs - id
        }

        copy(state = state.copy(podSpecs = newPodSpecs), dirtyPodIds = dirtyPodIds ++ Set(id))

      case ReservationSpecUpdated(id, _) =>
        throw new NotImplementedError("ReservationSpec support not yet implemented")
    }
  }

  def process(fn: (SchedulerLogicState, Set[PodId]) => SchedulerLogicIntents): FrameResultBuilder =
    applyAndAccumulate(fn(this.state, this.dirtyPodIds))

  lazy val result: FrameResult =
    FrameResult(stateEvents = appliedStateEvents, mesosIntents = mesosIntents)
}

object FrameResultBuilder {
  def forFrame(frame: SchedulerLogicState): FrameResultBuilder = FrameResultBuilder(frame, Nil, Nil, Set.empty)
}
