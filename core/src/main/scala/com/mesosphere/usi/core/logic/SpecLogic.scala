package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import com.typesafe.scalalogging.StrictLogging

/**
  * The current home for USI business logic
  *
  */
private[core] class SpecLogic(mesosCallFactory: MesosCalls) extends StrictLogging {
  import SchedulerLogicHelpers._

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
      .foldLeft(SchedulerEventsBuilder.empty) { (initialSchedulerEvents, podId) =>
        specs.podSpecs.get(podId) match {
          case None =>
            def maybePrunePodStatus(effects: SchedulerEventsBuilder) = {
              // This is spurious if e have a non-terminal podStatus
              val existingTerminalStatus = state.podStatuses.get(podId).exists(_.isTerminalOrUnreachable)
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

            maybePrunePodStatus(maybePruneRecord(initialSchedulerEvents))

          case Some(podSpec) =>
            if (podSpec.goal == Goal.Terminal) {
              taskIdsFor(podSpec).foldLeft(initialSchedulerEvents) { (effects, taskId) =>
                effects.withMesosCall(
                  mesosCallFactory.newKill(taskId.asProto, state.podRecords.get(podSpec.id).map(_.agentId.asProto), None))
              }
            } else {
              initialSchedulerEvents
            }
        }
      }
      .result
  }
}

