package com.mesosphere.usi.core

import java.time.Instant

import com.mesosphere.usi.core.models._
import com.mesosphere.usi.models._

case class FrameWithEffects(frame: Frame, effects: FrameEffects, dirtyPodIds: Set[PodId]) {
  def applyEffects(newEffects: FrameEffects): FrameWithEffects = {
    val newDirty = dirtyPodIds ++ newEffects.reverseStateEvents.iterator.collect {
      case podEvent: USIPodEvent => podEvent.id
    }
    copy(frame = frame.applyStateEffects(effects), dirtyPodIds = newDirty, effects = effects ++ newEffects)
  }

  def applySpecEvent(specEvent: SpecEvent): FrameWithEffects = {
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

        val changedPodIds = frame.podSpecs.keySet ++ newPodsSpecs.keySet

        copy(frame = frame.copy(podSpecs = newPodsSpecs), dirtyPodIds = dirtyPodIds ++ changedPodIds)

      case PodSpecUpdated(id, newState) =>
        val newPodSpecs = newState match {
          case Some(podSpec) =>
            frame.podSpecs.updated(id, podSpec)
          case None =>
            frame.podSpecs - id
        }

        copy(frame = frame.copy(podSpecs = newPodSpecs), dirtyPodIds = dirtyPodIds ++ Set(id))

      case ReservationSpecUpdated(id, _) =>
        throw new NotImplementedError("ReservationSpec support not yet implemented")
    }
  }

  def process(fn: FrameWithEffects => FrameEffects): FrameWithEffects = applyEffects(fn(this))
}

/**
  * Container class responsible for keeping track of the frame and cache.
  *
  * As a general rule, business logic does not manipulate the USI state directly, but rather does so by returning
  * effects.
  */
private[core] class SchedulerLogic {
  private var frame: Frame = Frame.empty
  private var cachedPendingLaunch = CachedPendingLaunch(Set.empty)

  def processSpecEvent(msg: SpecEvent): FrameEffects = {
    handleFrame { frameWithEffects =>
      frameWithEffects
        .applySpecEvent(msg)
        .process { f =>
          SchedulerLogic.computeNextStateForPods(f.frame)(f.dirtyPodIds)
        }
    }
  }

  /**
    * Instantiate a frameWithEffects instance, call the handler, then follow up with housekeeping:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * @return The total frame effects applied over the life-cycle of this frame evaluation.
    */
  private def handleFrame(fn: FrameWithEffects => FrameWithEffects): FrameEffects = {
    val result = fn(FrameWithEffects(this.frame, FrameEffects.empty, Set.empty)).process { f =>
      SchedulerLogic.pruneTaskStatuses(f.frame)(f.dirtyPodIds)
    }.process(updateCachesAndRevive)

    this.frame = result.frame
    result.effects
  }

  private def updateCachesAndRevive(frameWithEffects: FrameWithEffects): FrameEffects = {
    this.cachedPendingLaunch = this.cachedPendingLaunch.update(frameWithEffects.frame, frameWithEffects.dirtyPodIds)
    if (cachedPendingLaunch.pendingLaunch.nonEmpty)
      FrameEffects.empty.withMesosCall(Mesos.Call.Revive)
    else
      FrameEffects.empty
  }

  /**
    * Process a Mesos event and update internal state.
    *
    * @param event
    * @return The events describing state changes as Mesos call intents
    */
  def processMesosEvent(event: Mesos.Event): FrameEffects = {
    handleFrame { frameWithResult =>
      frameWithResult.process { f =>
        SchedulerLogic.handleMesosEvent(f.frame, cachedPendingLaunch.pendingLaunch)(event)
      }
    }
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

  def handleMesosEvent(frame: Frame, pendingLaunch: Set[PodId])(event: Mesos.Event): FrameEffects = {
    event match {
      case offer: Mesos.Event.Offer =>
        matchOffer(offer, pendingLaunch.flatMap { podId =>
          frame.podSpecs.get(podId)
        }(collection.breakOut))

      case Mesos.Event.StatusUpdate(taskId, taskStatus) =>
        val podId = podIdFor(taskId)

        if (frame.podSpecs.contains(podId)) {
          val newState = frame.podStatuses.get(podId) match {
            case Some(status) =>
              status.copy(taskStatuses = status.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          FrameEffects.empty
            .withPodStatus(podId, Some(newState))

        } else {
          FrameEffects.empty
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
