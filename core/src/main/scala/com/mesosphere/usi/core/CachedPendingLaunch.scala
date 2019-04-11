package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.{Goal, PodId, PodRecord}

case class UpdateResult(cachedPendingLaunch: CachedPendingLaunch, toBeLaunched: Set[PodId], launched: Set[PodId])

case class CachedPendingLaunch(pendingLaunch: Set[PodId] = Set.empty) {

  /**
    * Maintains a cache of pods pending launch, so a full scan isn't required on every offer
    *
    * Must be called after podSpecs are updated, and response effects are applied.
    *
    * @param podIds
    */
  def update(specs: SpecState, state: SchedulerState, podIds: Set[PodId]): UpdateResult = {
    var newPendingLaunch = pendingLaunch
    var newToBeLaunched = Set.empty[PodId]
    var newLaunched = Set.empty[PodId]
    podIds.foreach { podId =>
      val shouldBeLaunched = specs.podSpecs.get(podId).exists { podSpec =>
        pendingLaunch(podSpec.goal, state.podRecords.get(podId))
      }
      if (shouldBeLaunched) {
        if (!pendingLaunch.contains(podId)) {
          // this is new pod that is waiting to be launched but was not present in previous frame
          newToBeLaunched += podId
        }
        newPendingLaunch += podId
      } else {
        if (pendingLaunch.contains(podId)) {
          // this pod was pending launch but has already been launched in this frame
          newLaunched += podId
        }
        newPendingLaunch -= podId
      }
    }

    UpdateResult(CachedPendingLaunch(newPendingLaunch), newToBeLaunched, newLaunched)
  }

  private[core] def pendingLaunch(goal: Goal, podRecord: Option[PodRecord]): Boolean = {
    (goal == Goal.Running) && podRecord.isEmpty
  }
}
