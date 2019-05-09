package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._

/**
  * The current home for USI business logic for dealing with spec commands
  *
  */
private[core] class SpecLogic(mesosCallFactory: MesosCalls) {

  private[core] def handleCommand(state: SchedulerState)(command: SchedulerCommand): SchedulerEvents = {
    command match {
      case LaunchPod(id, runSpec) =>
        if (!state.podRecords.contains(id))
          SchedulerEvents(stateEvents = List(PodSpecUpdated(id, Some(RunningPodSpec(id, runSpec)))))
        else
          // if we already have a record for the pod, ignore
          SchedulerEvents.empty
      case ExpungePod(podId) =>
        var b = SchedulerEventsBuilder.empty
        if (state.podSpecs.contains(podId)) {
          b = b.withStateEvent(PodSpecUpdated(podId, None))
        }
        if (state.podRecords.contains(podId)) {
          b = b.withStateEvent(PodRecordUpdated(podId, None))
        }
        b.result
      case KillPod(podId) =>
        var b = SchedulerEventsBuilder.empty.withStateEvent(PodSpecUpdated(podId, Some(TerminalPodSpec(podId))))

        state.podStatuses.get(podId).foreach { status =>
          b = killPod(b, status)
        }
        b.result
      case _: CreateReservation =>
        ???
    }
  }

  private[core] def killPod(eventsBuilder: SchedulerEventsBuilder, podStatus: PodStatus) = {
    import com.mesosphere.usi.core.protos.ProtoConversions._
    podStatus.taskStatuses.foldLeft(eventsBuilder) {
      case (eventsBuilder, (taskId, status)) =>
        eventsBuilder.withMesosCall(
          mesosCallFactory
            .newKill(taskId.asProto, if (status.hasAgentId) Some(status.getAgentId) else None, None))
    }
  }
}
