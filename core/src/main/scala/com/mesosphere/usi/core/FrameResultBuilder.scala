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
case class FrameResultBuilder(specs: SpecState, state: SchedulerState, appliedStateEvents: List[StateEvent], mesosCalls: List[MesosCall], dirtyPodIds: Set[PodId]) {
  private def applyAndAccumulate(schedulerEvents: SchedulerEvents): FrameResultBuilder = {
    if (schedulerEvents == SchedulerEvents.empty)
      this
    else {
      // TODO - we need to handle status snapshots and create a mechanism to signal that all cache should be recomputed
      val newDirty = dirtyPodIds ++ schedulerEvents.stateEvents.iterator.collect {
        case podEvent: PodStateEvent => podEvent.id
      }
      copy(
        state = state.applyStateIntents(schedulerEvents.stateEvents),
        dirtyPodIds = newDirty,
        appliedStateEvents = appliedStateEvents ++ schedulerEvents.stateEvents,
        mesosCalls = mesosCalls ++ schedulerEvents.mesosCalls)
    }
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

        val changedPodIds = specs.podSpecs.keySet ++ newPodsSpecs.keySet

        copy(specs = specs.copy(podSpecs = newPodsSpecs), dirtyPodIds = dirtyPodIds ++ changedPodIds)

      case PodSpecUpdated(id, newState) =>
        val newPodSpecs = newState match {
          case Some(podSpec) =>
            specs.podSpecs.updated(id, podSpec)
          case None =>
            specs.podSpecs - id
        }

        copy(specs = specs.copy(podSpecs = newPodSpecs), dirtyPodIds = dirtyPodIds ++ Set(id))

      case ReservationSpecUpdated(id, _) =>
        throw new NotImplementedError("ReservationSpec support not yet implemented")
    }
  }

  def process(fn: (SpecState, SchedulerState, Set[PodId]) => SchedulerEvents): FrameResultBuilder =
    applyAndAccumulate(fn(this.specs, this.state, this.dirtyPodIds))

  lazy val result: SchedulerEvents =
    SchedulerEvents(stateEvents = appliedStateEvents, mesosCalls = mesosCalls)

  /**
    * Instantiate a frameResultBuilder instance, call the handler, then follow up with housekeeping:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index / cache
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * @return The total state effects applied over the life-cycle of this state evaluation.
    */
  def handleFrame(fn: FrameResultBuilder => FrameResultBuilder): SchedulerEvents = {
    val frameResultBuilder = fn(FrameResultBuilder.givenState(this.specs, this.state)).process {
      (specs, state, dirtyPodIds) =>
        pruneTaskStatuses(specs, state)(dirtyPodIds)
    }.process(updateCachesAndRevive)

    // update our state for the next frame processing
    this.state = frameResultBuilder.state
    this.specs = frameResultBuilder.specs

    // Return our result
    frameResultBuilder.result
  }

  /**
    * We remove a task if it is not reachable and running, and it has no podSpec defined
    *
    * Should be called with the effects already applied for the specified podIds
    *
    * @param podIds podIds changed during the last state
    * @return
    */
  def pruneTaskStatuses(specs: SpecState, state: SchedulerState)(
    podIds: Set[PodId]): SchedulerEvents = {
    podIds.iterator.filter { podId =>
      state.podStatuses.contains(podId)
    }.filter { podId =>
      val podSpecDefined = !specs.podSpecs.contains(podId)
      // prune terminal statuses for which there's no defined podSpec
      !podSpecDefined && terminalOrUnreachable(state.podStatuses(podId))
    }.foldLeft(SchedulerEventsBuilder.empty) { (effects, podId) =>
      effects.withPodStatus(podId, None)
    }
      .result
  }
}

object FrameResultBuilder {
  def givenState(specificationState: SpecState, state: SchedulerState): FrameResultBuilder =
    FrameResultBuilder(specificationState, state, Nil, Nil, Set.empty)
}
