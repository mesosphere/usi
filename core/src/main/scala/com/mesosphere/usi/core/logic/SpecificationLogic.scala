package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.{Protos => Mesos}

/**
  * The current home for USI business logic
  *
  * TODO separate the Mesos and the specification logic.
  */
private[core] class SpecificationLogic(mesosCallFactory: MesosCalls) extends StrictLogging {
  import SchedulerLogicHelpers._
  private def terminalOrUnreachable(status: PodStatus): Boolean = {
    // TODO - temporary stub implementation
    status.taskStatuses.values.forall(status => status.getState == Mesos.TaskState.TASK_RUNNING)
  }

  /**
    * Process the modification of some podSpec.
    *
    * - If a podSpec is deleted:
    *   - Also delete the podRecord.
    *   - Prune a terminal / unknown task status.
    * - If a podSpec is marked as terminal, then issue a kill.
    */
  private[core] def computeNextStateForPods(specs: SpecState, state: SchedulerState)(
      changedPodIds: Set[PodId]): SchedulerEvents = {
    import com.mesosphere.usi.core.protos.ProtoConversions._
    changedPodIds
      .foldLeft(SchedulerEventsBuilder.empty) { (initialIntents, podId) =>
        specs.podSpecs.get(podId) match {
          case None =>
            // TODO - this should be spurious if the podStatus is non-terminal
            def maybePrunePodStatus(effects: SchedulerEventsBuilder) = {
              val existingTerminalStatus = state.podStatuses.get(podId).exists(terminalOrUnreachable(_))
              if (existingTerminalStatus) {
                effects.withPodStatus(podId, None)
              } else {
                effects
              }
            }

            def maybePruneRecord(effects: SchedulerEventsBuilder) = {
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
                effects.withMesosCall(
                  mesosCallFactory.newKill(taskId.asProto, state.podRecords.get(podSpec.id).map(_.agentId.asProto), None))
              }
            } else {
              initialIntents
            }
        }
      }
      .result
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

