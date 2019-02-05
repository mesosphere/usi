package com.mesosphere.usi.core

import java.time.Instant

import com.mesosphere.usi.core.models._
import com.mesosphere.usi.models._

/**
  * Container class responsible for keeping track of the frame and cache.
  *
  * As a general rule, business logic does not manipulate the USI state directly, but rather does so by returning
  * effects.
  */
private[core] class SchedulerLogic {
  private var frame: Frame = Frame(Map.empty, Map.empty, Map.empty)
  private var cachedPendingLaunch = CachedPendingLaunch(Set.empty)

  def processSpecEvent(msg: SpecEvent): FrameEffects = {
    val (frameWithPodSpecChanges, changedPodIds) = frame.applySpecEvent(msg)
    frame = frameWithPodSpecChanges
    val frameEffects = SchedulerLogic.computeNextStateForPods(frameWithPodSpecChanges)(changedPodIds)
    applyAndUpdateCaches(frameEffects, changedPodIds)
  }

  /**
    * Given a frame effects, apply the usi state change effects of the frame effects. Then, update the cache and perform
    * other housekeeping items for pods that are dirty, such as:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * Any usiState changes (such as pruning terminal / unreachable podStatuses) are added to the frameEffects.
    *
    * @param eventFrameEffects The effects from the incoming SpecEvent
    * @param dirtyPodIds The podSpecs whose cache should be updated or checked for pruning
    * @return The frame effects with additional housekeeping effects applied
    */
  private def applyAndUpdateCaches(eventFrameEffects: FrameEffects, dirtyPodIds: Set[PodId]): FrameEffects = {
    frame = frame.applyStateEffects(eventFrameEffects)
    this.cachedPendingLaunch = this.cachedPendingLaunch.update(frame, dirtyPodIds)

    val pruneEffects = SchedulerLogic.pruneTaskStatuses(frame)(dirtyPodIds)
    frame = frame.applyStateEffects(pruneEffects)

    val reviveEffects =
      if (cachedPendingLaunch.pendingLaunch.nonEmpty)
        FrameEffects.empty.withMesosCall(Mesos.Call.Revive)
      else
        FrameEffects.empty

    eventFrameEffects ++ pruneEffects ++ reviveEffects
  }

  /**
    * Process a Mesos event and update internal state.
    *
    * @param event
    * @return The events describing state changes as Mesos call intents
    */
  def processMesosEvent(event: Mesos.Event): FrameEffects = {
    val result = SchedulerLogic.handleMesosEvent(frame, cachedPendingLaunch.pendingLaunch)(event)

    applyAndUpdateCaches(result.frameEffects, result.dirtyPodIds)
  }
}

/**
  * The current home for USI business logic
  */
private[core] object SchedulerLogic {
  private def terminalOrUnreachable(status: PodStatus): Boolean = {
    // TODO - temporary stub implementation
    status.taskStatuses.values.forall(status => status == Mesos.TaskStatus.TASK_RUNNING)
  }

  private def taskIdsFor(pod: PodSpec): Seq[TaskId] = {
    // TODO - temporary stub implementation
    Seq(pod.id.value)
  }

  /**
    * Process the modification of some podSpec.
    *
    * - If a podSpec is deleted:
    *   - Also delete the podRecord.
    *   - Prune a terminal / unknown task status.
    * - If a podSpec is marked as terminal, then issue a kill.
    *
    * @return The effects of launching this offer
    */
  private def matchOffer(offer: Mesos.Event.Offer, pendingLaunchPodSpecs: Seq[PodSpec]): FrameEffects = {
    var effects = FrameEffects.empty

    val operations = pendingLaunchPodSpecs.iterator.flatMap { pod =>
      taskIdsFor(pod).iterator.map { taskId =>
        Mesos.Operation(Mesos.Launch(Mesos.TaskInfo(taskId)))
      }
    }.to[Seq]

    pendingLaunchPodSpecs.foreach { pod =>
      effects = effects.withPodRecord(pod.id, Some(PodRecord(pod.id, Instant.now(), offer.agentId)))
    }
    effects = effects.withMesosCall(Mesos.Call.Accept(offer.offerId, operations = operations))

    effects
  }

  private[core] def pendingLaunch(goal: Goal, podRecord: Option[PodRecord]): Boolean = {
    (goal == Goal.Running) && podRecord.isEmpty
  }

  private[core] def computeNextStateForPods(frame: Frame)(changedPodIds: Set[PodId]): FrameEffects = {
    changedPodIds.foldLeft(FrameEffects.empty) { (initialEffects, podId) =>
      var effects = initialEffects

      frame.podSpecs.get(podId) match {
        case None =>
          // TODO - this should be spurious if the podStatus is non-terminal
          if (frame.podStatuses.get(podId).exists { status =>
              terminalOrUnreachable(status)
            }) {
            effects = effects.withPodStatus(podId, None)
          }

          if (frame.podRecords.contains(podId)) {
            // delete podRecord
            effects = effects.withPodRecord(podId, None)
          }

        case Some(podSpec) =>
          if (podSpec.goal == Goal.Terminal) {
            taskIdsFor(podSpec).foreach { taskId =>
              effects = effects.withMesosCall(Mesos.Call.Kill(taskId))
            }
          }
      }
      effects
    }
  }

  case class HandleMesosEventResult(frameEffects: FrameEffects, dirtyPodIds: Set[PodId])

  object HandleMesosEventResult {
    val empty = HandleMesosEventResult(FrameEffects.empty, Set.empty)
  }

  def handleMesosEvent(frame: Frame, pendingLaunch: Set[PodId])(event: Mesos.Event): HandleMesosEventResult = {
    event match {
      case offer: Mesos.Event.Offer =>
        val changedPodIds = pendingLaunch
        val effects = matchOffer(offer, pendingLaunch.flatMap { podId =>
          frame.podSpecs.get(podId)
        }(collection.breakOut))
        HandleMesosEventResult(effects, changedPodIds)

      case Mesos.Event.StatusUpdate(taskId, taskStatus) =>
        val podId = podIdFor(taskId)

        if (frame.podSpecs.contains(podId)) {
          val newState = frame.podStatuses.get(podId) match {
            case Some(status) =>
              status.copy(taskStatuses = status.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          val effects = FrameEffects.empty
            .withPodStatus(podId, Some(newState))

          HandleMesosEventResult(effects, Set(podId))
        } else {
          HandleMesosEventResult.empty
        }
    }
  }

  /**
    * We remove a task if it is not reachable and running, and it has no podSpec defined
    *
    * Should be called with the effects already applied for the specified podIds
    *
    * @param podIds podIds changed during the last frame
    * @return
    */
  def pruneTaskStatuses(frame: Frame)(podIds: Set[PodId]): FrameEffects = {
    var effects = FrameEffects.empty
    podIds.foreach { podId =>
      if (frame.podStatuses.contains(podId)) {
        val podSpecDefined = !frame.podSpecs.contains(podId)
        // prune terminal statuses for which there's no defined podSpec
        if (!podSpecDefined && terminalOrUnreachable(frame.podStatuses(podId))) {
          effects = effects.withPodStatus(podId, None)
        }
      }
    }
    effects
  }

  private def podIdFor(taskId: TaskId): PodId = PodId(taskId)
}
