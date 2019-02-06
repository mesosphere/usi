package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.PodId

case class CachedPendingLaunch(pendingLaunch: Set[PodId] = Set.empty) {

  /**
    * Maintains a cache of pods pending launch, so a full scan isn't required on every offer
    *
    * Must be called after podSpecs are updated, and response effects are applied.
    *
    * @param podIds
    */
  def update(state: SchedulerLogicState, podIds: Set[PodId]): CachedPendingLaunch = {
    var newPendingLaunch = pendingLaunch
    podIds.foreach { podId =>
      val shouldBeLaunched = state.podSpecs.get(podId).exists { podSpec =>
        SchedulerLogic.pendingLaunch(podSpec.goal, state.podRecords.get(podId))
      }
      if (shouldBeLaunched)
        newPendingLaunch += podId
      else
        newPendingLaunch -= podId
    }

    CachedPendingLaunch(newPendingLaunch)
  }
}
