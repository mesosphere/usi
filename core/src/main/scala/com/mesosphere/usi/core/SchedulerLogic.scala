package com.mesosphere.usi.core

import java.time.Instant

import com.mesosphere.usi.core.models._

/**
  * Container class responsible for keeping track of the state and cache.
  *
  * ## SpecificationState vs SchedulerLogic state
  *
  * The SchedulerLogic has two pieces of state: SpecificationState and SchedulerLogicState (records and statuses). The
  * SchedulerLogic maintains the latter. All manipulation to the SchedulerLogic state is done via one of the two
  * processes:
  *
  * - The specification state is updated through incoming SpecEvents (we consume and replicate the
  * framework-implementation's specifications)
  * - The SchedulerLogicState (statuses, records, etc.) is updated through StateEvents that returned as intents
  *
  * As such, it's worth emphasizing that business logic does not have any direct side-effects, and it manipulates the
  * SchedulerLogicState only by returning intents. This allows us the following:
  *
  * - It's more efficient, since it saves us the trouble of diffing a large data-structure with each update
  * - We have built-in guarantees that evolutions to the SchedulerLogicState can be reliably replicated by processing
  * these events, since they led to the changes in the first place
  * - We can easily know which portions of the SchedulerLogicState should be persisted during the persistence layer.
  * - It restricts, via the type system, the portions of the state the business logic is allowed (IE: it
  * would be illegal for the business logic to update a specification)
  *
  * ## Intents and Events
  *
  * In the SchedulerLogic code, we'll use the word intents and events. Intents are things not yet applied, and should
  * be. Events are things that were applied and we're notifying you about.
  *
  * In the SchedulerLogic, a StateEvent is used both to manipulate the SchedulerLogicState (similar to how
  * event-sourced persistent actors evolve their state), and is also used to describe the evolution (so that the state
  * can be incrementally persisted and followed). In the SchedulerLogic, we'll refer to a StateEvent as an intent until
  * it is applied, after-which it will be called an event. Mesos calls will be referred to as intents as they are not
  * applied until they are published to the Mesos Master.
  *
  * ## Concept of a "Frame"
  *
  * Visualized:
  *
  * [Incoming Event: offer match]
  *      |
  *      v
  * (beginning of frame)
  *   apply pod launch logic:
  *     emit Mesos accept offer call for matched pending launch pods
  *     specify the existence of a podRecord, with the launch time and agentId
  *   update internal cache
  *   process revive / suppress need
  * (end of frame)
  *      |
  *      v
  * [Emit all state events and Mesos call intents]
  *
  * In the SchedulerLogic, each event is processed (either a Specification updated event, or a Mesos event) in what we
  * call a "frame". During the processing of a single event (such as an offer), the SchedulerLogic will be updated
  * through the application of several functions. The result of that event will lead to the emission of several
  * stateEvents and Mesos intents, which will be accumulated and emitted via a data structure we call the FrameResult.
  */
private[core] class SchedulerLogic {
  /**
    * Our view of the framework-implementations specifications, which we replicate by consuming Specification events
    */
  private var specs: SpecificationState = SpecificationState.empty
  /**
    * State managed by the SchedulerLogic (records and statuses). Statuses are derived from Mesos events. Records
    * contain persistent, non-recoverable facts from Mesos, such as pod-launched time, agentId on which a pod was
    * launched, agent information or the time at which a pod was first seen as unhealthy or unreachable.
    */
  private var state: SchedulerLogicState = SchedulerLogicState.empty

  /**
    * Cached view of SchedulerLogicState and SpecificationState. We incrementally update this computation at the end of
    * each frame based on podIds becoming dirty.
    */
  private var cachedPendingLaunch = CachedPendingLaunch(Set.empty)

  def processSpecEvent(msg: SpecEvent): FrameResult = {
    handleFrame { builder =>
      builder
        .applySpecEvent(msg)
        .process { (specs, state, dirtyPodIds) =>
          SchedulerLogic.computeNextStateForPods(specs, state)(dirtyPodIds)
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
    val frameResultBuilder = fn(FrameResultBuilder.givenState(this.specs, this.state)).process { (specs, state, dirtyPodIds) =>
      SchedulerLogic.pruneTaskStatuses(specs, state)(dirtyPodIds)
    }.process(updateCachesAndRevive)

    // update our state for the next frame processing
    this.state = frameResultBuilder.state
    this.specs = frameResultBuilder.specs

    // Return our result
    frameResultBuilder.result
  }

  private def updateCachesAndRevive(specs: SpecificationState, state: SchedulerLogicState, dirtyPodIds: Set[PodId]): SchedulerLogicIntents = {
    this.cachedPendingLaunch = this.cachedPendingLaunch.update(specs, state, dirtyPodIds)
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
      builder.process { (specs, state, _) =>
        SchedulerLogic.handleMesosEvent(specs, state, cachedPendingLaunch.pendingLaunch)(event)
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
  private[core] def computeNextStateForPods(specs: SpecificationState, state: SchedulerLogicState)(changedPodIds: Set[PodId]): SchedulerLogicIntents = {
    changedPodIds.foldLeft(SchedulerLogicIntentsBuilder.empty) { (initialIntents, podId) =>
      specs.podSpecs.get(podId) match {
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

  def handleMesosEvent(specs: SpecificationState, state: SchedulerLogicState, pendingLaunch: Set[PodId])(event: Mesos.Event): SchedulerLogicIntents = {
    event match {
      case offer: Mesos.Event.Offer =>
        matchOffer(offer, pendingLaunch.flatMap { podId =>
          specs.podSpecs.get(podId)
        }(collection.breakOut))

      case Mesos.Event.StatusUpdate(taskId, taskStatus) =>
        val podId = podIdFor(taskId)

        if (specs.podSpecs.contains(podId)) {
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
  def pruneTaskStatuses(specs: SpecificationState, state: SchedulerLogicState)(podIds: Set[PodId]): SchedulerLogicIntents = {
    podIds.iterator.filter { podId =>
      state.podStatuses.contains(podId)
    }.filter { podId =>
      val podSpecDefined = !specs.podSpecs.contains(podId)
      // prune terminal statuses for which there's no defined podSpec
      !podSpecDefined && terminalOrUnreachable(state.podStatuses(podId))
    }.foldLeft(SchedulerLogicIntentsBuilder.empty) { (effects, podId) =>
      effects.withPodStatus(podId, None)
    }.result
  }

  private def podIdFor(taskId: TaskId): PodId = PodId(taskId.value)
}
