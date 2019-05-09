package com.mesosphere.usi.core
import com.mesosphere.usi.core.models._
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall}

/**
  * Immutable helper data structure which:
  *
  * - applies state events to the frame to update our snapshot of the state; accumulates them for publishing
  * - accumulates mesos calls for publishing
  * - keeps track of which podIds have changed state (for cache invalidation)
  *
  * This data structure enables various stages of the transformation of a frame to be expressed via a pipeline of chained
  * calls
  *
  * @param state The current SchedulerLogic state
  * @param appliedStateEvents The cumulative stateEvents applied to the frame; will be included in the FrameResult
  * @param mesosCalls The cumulative Mesos calls that are intended to be sent
  * @param dirtyPodIds The podIds that have been changed during the lifecycle of this FrameWithEvents instance
  */
case class FrameResultBuilder(
    state: SchedulerState,
    appliedStateEvents: List[StateEvent],
    mesosCalls: List[MesosCall],
    dirtyPodIds: Set[PodId]) {
  private def applyAndAccumulate(schedulerEvents: SchedulerEvents): FrameResultBuilder = {
    if (schedulerEvents == SchedulerEvents.empty)
      this
    else {
      val newDirty: Set[PodId] = dirtyPodIds ++ schedulerEvents.stateEvents.iterator
        .collect[Set[PodId]] {
          case podEvent: PodStateEvent => Set(podEvent.id)
          // We need to handle status snapshots with a mechanism to signal that all cache should be recomputed
          case snapshot: StateSnapshot => snapshot.podRecords.map(_.podId).toSet
        }
        .flatten

      copy(
        state = state.applyStateIntents(schedulerEvents.stateEvents),
        dirtyPodIds = newDirty,
        appliedStateEvents = appliedStateEvents ++ schedulerEvents.stateEvents,
        mesosCalls = mesosCalls ++ schedulerEvents.mesosCalls
      )
    }
  }

  def process(fn: (SchedulerState, Set[PodId]) => SchedulerEvents): FrameResultBuilder =
    applyAndAccumulate(fn(this.state, this.dirtyPodIds))

  lazy val result: SchedulerEvents =
    SchedulerEvents(stateEvents = appliedStateEvents, mesosCalls = mesosCalls)
}

object FrameResultBuilder {
  def givenState(state: SchedulerState): FrameResultBuilder =
    FrameResultBuilder(state, Nil, Nil, Set.empty)
}
