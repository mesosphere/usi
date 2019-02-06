package com.mesosphere.usi.core

import java.time.Instant

import com.mesosphere.usi.core.models._

/**
  * Container class responsible for keeping track of the state and cache.
  *
  * As a general rule, business logic does not manipulate the USI state directly, but rather does so by returning
  * effects.
  */
private[core] class SchedulerLogic {
  private var state: SchedulerLogicState = SchedulerLogicState.empty
  private var cachedPendingLaunch = CachedPendingLaunch(Set.empty)

  def processSpecEvent(msg: SpecEvent): FrameResult = {
    handleFrame { builder =>
      builder
        .applySpecEvent(msg)
        .process { (state, dirtyPodIds) =>
          SchedulerLogic.computeNextStateForPods(state)(dirtyPodIds)
        }
    }
  }

  /**
    * Instantiate a frameResultBuilder instance, call the handler, then follow up with housekeeping:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index / cache
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * @return The total state effects applied over the life-cycle of this state evaluation.
    */
  private def handleFrame(fn: FrameResultBuilder => FrameResultBuilder): FrameResult = {
    val frameResultBuilder = fn(FrameResultBuilder.givenState(this.state)).process { (state, dirtyPodIds) =>
      SchedulerLogic.pruneTaskStatuses(state)(dirtyPodIds)
    }.process(updateCachesAndRevive)

    // update our state for the next process
    this.state = frameResultBuilder.state

    // Return our result
    frameResultBuilder.result
  }

  private def updateCachesAndRevive(state: SchedulerLogicState, dirtyPodIds: Set[PodId]): SchedulerLogicIntents = {
    this.cachedPendingLaunch = this.cachedPendingLaunch.update(state, dirtyPodIds)
    if (cachedPendingLaunch.pendingLaunch.nonEmpty)
      SchedulerLogicIntents(mesosIntents = List(Mesos.Call.Revive))
    else
      SchedulerLogicIntents.empty
  }

  /**
    * Process a Mesos event and update internal state.
    *
    * @param event
    * @return The events describing state changes as Mesos call intents
    */
  def processMesosEvent(event: Mesos.Event): FrameResult = {
    handleFrame { builder =>
      builder.process { (state, _) =>
        SchedulerLogic.handleMesosEvent(state, cachedPendingLaunch.pendingLaunch)(event)
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
    Seq(TaskId(pod.id.value))
  }

  private def matchOffer(offer: Mesos.Event.Offer, pendingLaunchPodSpecs: Seq[PodSpec]): SchedulerLogicIntents = {
    val operations = pendingLaunchPodSpecs.iterator.flatMap { pod =>
      taskIdsFor(pod).iterator.map { taskId =>
        Mesos.Operation(Mesos.Launch(Mesos.TaskInfo(taskId)))
      }
    }.to[Seq]

    val intentsBuilder = pendingLaunchPodSpecs.foldLeft(SchedulerLogicIntentsBuilder.empty) { (effects, pod) =>
      effects.withPodRecord(pod.id, Some(PodRecord(pod.id, Instant.now(), offer.agentId)))
    }
    intentsBuilder.withMesosCall(Mesos.Call.Accept(offer.offerId, operations = operations)).result
  }

  private[core] def pendingLaunch(goal: Goal, podRecord: Option[PodRecord]): Boolean = {
    (goal == Goal.Running) && podRecord.isEmpty
  }

  /**
    * Process the modification of some podSpec.
    *
    * - If a podSpec is deleted:
    *   - Also delete the podRecord.
    *   - Prune a terminal / unknown task status.
    * - If a podSpec is marked as terminal, then issue a kill.
    */
  private[core] def computeNextStateForPods(state: SchedulerLogicState)(changedPodIds: Set[PodId]): SchedulerLogicIntents = {
    changedPodIds.foldLeft(SchedulerLogicIntentsBuilder.empty) { (initialIntents, podId) =>
      state.podSpecs.get(podId) match {
        case None =>
          // TODO - this should be spurious if the podStatus is non-terminal
          def maybePrunePodStatus(effects: SchedulerLogicIntentsBuilder) = {
            val existingTerminalStatus = state.podStatuses.get(podId).exists(terminalOrUnreachable(_))
            if (existingTerminalStatus) {
              effects.withPodStatus(podId, None)
            } else {
              effects
            }
          }

          def maybePruneRecord(effects: SchedulerLogicIntentsBuilder) = {
            if (state.podRecords.contains(podId)) {
              // delete podRecord
              effects.withPodRecord(podId, None)
            } else {
              effects
            }
          }

          maybePrunePodStatus(maybePruneRecord(initialIntents))

        case Some(podSpec) =>
          if (podSpec.goal == Goal.Terminal) {
            taskIdsFor(podSpec).foldLeft(initialIntents) { (effects, taskId) =>
              effects.withMesosCall(Mesos.Call.Kill(taskId))
            }
          } else {
            initialIntents
          }
      }
    }.result
  }

  def handleMesosEvent(state: SchedulerLogicState, pendingLaunch: Set[PodId])(event: Mesos.Event): SchedulerLogicIntents = {
    event match {
      case offer: Mesos.Event.Offer =>
        matchOffer(offer, pendingLaunch.flatMap { podId =>
          state.podSpecs.get(podId)
        }(collection.breakOut))

      case Mesos.Event.StatusUpdate(taskId, taskStatus) =>
        val podId = podIdFor(taskId)

        if (state.podSpecs.contains(podId)) {
          val newState = state.podStatuses.get(podId) match {
            case Some(status) =>
              status.copy(taskStatuses = status.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          SchedulerLogicIntents(
            stateIntents = List(PodStatusUpdated(podId, Some(newState))))

        } else {
          SchedulerLogicIntents.empty
        }
    }
  }

  /**
    * We remove a task if it is not reachable and running, and it has no podSpec defined
    *
    * Should be called with the effects already applied for the specified podIds
    *
    * @param podIds podIds changed during the last state
    * @return
    */
  def pruneTaskStatuses(state: SchedulerLogicState)(podIds: Set[PodId]): SchedulerLogicIntents = {
    podIds.iterator.filter { podId =>
      state.podStatuses.contains(podId)
    }.filter { podId =>
      val podSpecDefined = !state.podSpecs.contains(podId)
      // prune terminal statuses for which there's no defined podSpec
      !podSpecDefined && terminalOrUnreachable(state.podStatuses(podId))
    }.foldLeft(SchedulerLogicIntentsBuilder.empty) { (effects, podId) =>
      effects.withPodStatus(podId, None)
    }.result
  }

  private def podIdFor(taskId: TaskId): PodId = PodId(taskId.value)
}
