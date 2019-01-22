package com.mesosphere.usi.core

import java.time.Instant

import com.mesosphere.usi.core.models._
import com.mesosphere.usi.models._

private[core] object SchedulerLogic {
  private def terminalOrUnreachable(status: PodStatus): Boolean = {
    // Cheesy method
    status.taskStatuses.values.forall(status => status == Mesos.TaskStatus.TASK_RUNNING)
  }

  private def assertValidTransition(oldSpec: PodSpec, newSpec: PodSpec): Unit = {
    if ((oldSpec.goal == Goal.Terminal) && (newSpec.goal == Goal.Running))
      throw new IllegalStateException(s"Illegal state transition Terminal to Running for podId ${oldSpec.id}")
  }

  private def taskIdsFor(pod: PodSpec): Seq[TaskId] = {
    // ignoring momentarily that these might be different
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
    * @param podSpecs      The podSpecs state for this frame
    * @param podRecords    The current
    * @param podId
    * @param newState      The new state for this pod; optional. None for deleted.
    * @param resultBuilder The frameResult builder. Effects are appended on to this and returned.
    * @return The given responseBuilder plus all effects as a result of the application of this podSpecUpdate
    */
  def handlePodSpecUpdate(podSpecs: Map[PodId, PodSpec], podRecords: Map[PodId, PodRecord], podStatuses: Map[PodId, PodStatus])
                                 (podId: PodId, newState: Option[PodSpec], resultBuilder: FrameResultBuilder): FrameResultBuilder = {
    var b = resultBuilder

    newState match {
      case None =>
        // TODO - this should be spurious if the podStatus is non-terminal
        if (podStatuses.get(podId).exists { status => terminalOrUnreachable(status) }) {
          b = b.withChangeMessage(PodStatusUpdated(podId, None))
        }

        if (podRecords.contains(podId)) {
          b = b.withChangeMessage(PodRecordUpdated(podId, None))
        }

      case Some(podSpec) =>

        podSpecs.get(podId).foreach(assertValidTransition(_, podSpec))

        if (podSpec.goal == Goal.Terminal) {
          taskIdsFor(podSpec).foreach { taskId =>
            b = b.withMesosCall(Mesos.Call.Kill(taskId))
          }
        }
    }
    b
  }


  private def matchOffer(podSpecs: Map[PodId, PodSpec])(offer: Mesos.Event.Offer, pendingLaunch: Set[PodId]): FrameResultBuilder = {
    var b = FrameResultBuilder.empty

    val operations = pendingLaunch
      .iterator
      .flatMap { podId =>
        taskIdsFor(podSpecs(podId)).iterator.map { taskId =>
          Mesos.Operation(Mesos.Launch(Mesos.TaskInfo(taskId)))
        }
      }
      .to[Seq]

    pendingLaunch.foreach { podId =>
      b = b.withChangeMessage(PodRecordUpdated(podId, Some(PodRecord(podId, Instant.now(), offer.agentId))))
    }
    b = b.withMesosCall(Mesos.Call.Accept(offer.offerId, operations = operations))

    b
  }

  private[core] def pendingLaunch(podId: PodId, goal: Goal, podRecord: Option[PodRecord]): Boolean = {
    (goal == Goal.Running) && podRecord.isEmpty
  }

  private[core] case class HandleSpecEventResult(newPodSpecs: Map[PodId, PodSpec], dirtyPodIds: Set[PodId], frameResult: FrameResultBuilder)

  private[core] def handleSpecEvent(podRecords: Map[PodId, PodRecord], podStatuses: Map[PodId, PodStatus])(podSpecs: Map[PodId, PodSpec], msg: SpecEvent): HandleSpecEventResult = {
    msg match {
      case SpecsSnapshot(podSpecSnapshot, reservationSpecSnapshot) =>
        if (reservationSpecSnapshot.nonEmpty) {
          throw new NotImplementedError("ReservationSpec support not yet implemented")
        }
        val newPodsSpecs: Map[PodId, PodSpec] = podSpecSnapshot.map { pod => pod.id -> pod }(collection.breakOut)

        val changedPodIds = podSpecs.keySet ++ newPodsSpecs.keySet

        val responseFromUpdate = changedPodIds.foldLeft(FrameResultBuilder.empty) { (response, podId) =>
          SchedulerLogic.handlePodSpecUpdate(podSpecs, podRecords, podStatuses)(podId, newPodsSpecs.get(podId), response)
        }

        HandleSpecEventResult(
          newPodsSpecs,
          changedPodIds,
          responseFromUpdate)

      case PodSpecUpdated(id, newState) =>
        val newPodSpecs = newState match {
          case Some(podSpec) =>
            podSpecs.updated(id, podSpec)
          case None =>
            podSpecs - id
        }
        val responseFromUpdate = SchedulerLogic.handlePodSpecUpdate(podSpecs, podRecords, podStatuses)(id, newState, FrameResultBuilder.empty)

        HandleSpecEventResult(
          newPodSpecs,
          Set(id),
          responseFromUpdate)

      case ReservationSpecUpdated(id, _) =>
        throw new NotImplementedError("ReservationSpec support not yet implemented")
    }
  }

  case class HandleMesosEventResult(frameResult: FrameResultBuilder, dirtyPodIds: Set[PodId])

  object HandleMesosEventResult {
    val empty = HandleMesosEventResult(FrameResultBuilder.empty, Set.empty)
  }

  def handleMesosEvent(podStatuses: Map[PodId, PodStatus], podSpecs: Map[PodId, PodSpec], cachedPendingLaunch: Set[PodId])
                      (event: Mesos.Event): HandleMesosEventResult = {
    event match {
      case offer: Mesos.Event.Offer =>
        val changedPodIds = cachedPendingLaunch
        val response = matchOffer(podSpecs)(offer, cachedPendingLaunch)
        HandleMesosEventResult(response, changedPodIds)

      case Mesos.Event.StatusUpdate(taskId, taskStatus) =>

        val podId = podIdFor(taskId)

        if (podSpecs.contains(podId)) {
          val newState = podStatuses.get(podId) match {
            case Some(status) =>
              status.copy(taskStatuses = status.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          val response = FrameResultBuilder.empty.withChangeMessage(PodStatusUpdated(podId, Some(newState)))
          HandleMesosEventResult(
            response,
            Set(podId))
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
  private[core] def pruneTaskStatuses(podStatuses: Map[PodId, PodStatus], podSpecs: Map[PodId, PodSpec])
                                     (podIds: Set[PodId]): FrameResultBuilder = {
    var b = FrameResultBuilder.empty
    podIds.foreach { podId =>
      if (podStatuses.contains(podId)) {
        val podSpecDefined = !podSpecs.contains(podId)
        // prune terminal statuses for which there's no defined podSpec
        if (!podSpecDefined && terminalOrUnreachable(podStatuses(podId))) {
          b = b.withChangeMessage(PodStatusUpdated(podId, None))
        }
      }
    }
    b
  }

  private def podIdFor(taskId: TaskId): PodId = PodId(taskId)
}

/**
  * Container class responsible for keeping track of the USI state and podSpecs.
  *
  * As a general rule, business logic does not manipulate the USI state directly, but rather does so by returning intents.
  */
private[core] class SchedulerLogic {
  private var podSpecs: Map[PodId, PodSpec] = Map.empty
  private var podRecords: Map[PodId, PodRecord] = Map.empty
  private var podStatuses: Map[PodId, PodStatus] = Map.empty
  private var cachedPendingLaunch: Set[PodId] = Set.empty

  private def applyUSIStateChanges(response: FrameResult): Unit = {
    response.usiStateEvents.foreach {
      case recordChange: PodRecordUpdated =>
        recordChange.newRecord match {
          case Some(newRecord) =>
            podRecords = podRecords.updated(recordChange.id, newRecord)
            // podRecord presence means we've launched
            cachedPendingLaunch -= recordChange.id
          case None =>
            podRecords -= recordChange.id
        }
      case statusChange: PodStatusUpdated =>
        statusChange.newStatus match {
          case Some(newStatus) =>
            podStatuses = podStatuses.updated(statusChange.id, newStatus)
          case None =>
            podStatuses -= statusChange.id
        }
      case agentRecordChange: AgentRecordUpdated => // TODO
      case reservationStatusChange: ReservationStatusUpdated => // TODO
      case statusSnapshot: USIStateSnapshot => // TODO
    }
  }

  /**
    * Maintains the internal cache of pods pending launch, so a full scan isn't required on every offer
    *
    * Must be called after podSpecs are updated, and response effects are applied.
    *
    * @param podIds
    */
  private def updatePendingLaunch(podIds: Set[PodId]): Unit = {
    podIds.foreach { podId =>
      val shouldBeLaunched = podSpecs.get(podId).exists { podSpec =>
        SchedulerLogic.pendingLaunch(podId, podSpec.goal, podRecords.get(podId))
      }
      if (shouldBeLaunched)
        cachedPendingLaunch += podId
      else
        cachedPendingLaunch -= podId
    }
  }

  def processSpecEvent(msg: SpecEvent): FrameResult = {
    val handleResult = SchedulerLogic.handleSpecEvent(this.podRecords, this.podStatuses)(this.podSpecs, msg)
    this.podSpecs = handleResult.newPodSpecs

    applyAndUpdateCaches(handleResult.frameResult, handleResult.dirtyPodIds)
  }

  /**
    * Given a frameResult, apply the effects of the frame result. Then, update the cache and perform other housekeeping
    * items for pods that are dirty, such as:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * Any usiState changes (such as pruning terminal / unreachable podStatuses) are added to the frameResult.
    *
    * @param frameResult   The result from the incoming SpecEvent or
    * @param changedPodIds The
    * @return The frameResult with additional housekeeping effects applied
    */
  private def applyAndUpdateCaches(frameResult: FrameResultBuilder, changedPodIds: Set[PodId]): FrameResult = {
    applyUSIStateChanges(frameResult)

    val pruneResponse = SchedulerLogic.pruneTaskStatuses(this.podStatuses, this.podSpecs)(changedPodIds)
    applyUSIStateChanges(pruneResponse)

    updatePendingLaunch(changedPodIds)

    val reviveIfNonEmpty = if (cachedPendingLaunch.nonEmpty)
      FrameResultBuilder.empty.withMesosCall(Mesos.Call.Revive)
    else
      FrameResultBuilder.empty

    frameResult ++ pruneResponse ++ reviveIfNonEmpty
  }

  /**
    * Process a Mesos event and update internal state.
    *
    * @param event
    * @return The events describing state changes as Mesos call intents
    */
  def processMesosEvent(event: Mesos.Event): FrameResult = {
    val result = SchedulerLogic.handleMesosEvent(podStatuses, podSpecs, cachedPendingLaunch)(event)

    applyAndUpdateCaches(result.frameResult, result.dirtyPodIds)
  }
}
