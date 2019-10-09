package com.mesosphere.usi.core.logic

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.commands.{CreateReservation, ExpungePod, KillPod, LaunchPod, SchedulerCommand}

/**
  * The current home for USI business logic for dealing with spec commands
  *
  */
private[core] class SpecLogic(mesosCallFactory: MesosCalls) {

  private def getRunningPodSpec(podSpecs: Map[PodId, PodSpec], id: PodId): Option[RunningPodSpec] = {
    podSpecs.get(id).collect { case r: RunningPodSpec => r }
  }
  private[core] def handleCommand(state: SchedulerState)(command: SchedulerCommand): SchedulerEvents = {
    command match {
      case LaunchPod(id, runSpec) =>
        if (state.podRecords.contains(id) || getRunningPodSpec(state.podSpecs, id).exists(_.runSpec == runSpec)) {
          // if we already have a record for the pod, ignore
          SchedulerEvents.empty
        } else {
          SchedulerEvents(stateEvents = List(PodSpecUpdatedEvent(id, Some(RunningPodSpec(id, runSpec)))))
        }
      case ExpungePod(podId) =>
        var b = SchedulerEventsBuilder.empty
        if (state.podSpecs.contains(podId)) {
          b = b.withStateEvent(PodSpecUpdatedEvent(podId, None))
        }
        if (state.podRecords.contains(podId)) {
          b = b.withStateEvent(PodRecordUpdatedEvent(podId, None))
        }
        b.result
      case KillPod(podId) =>
        var b = SchedulerEventsBuilder.empty.withStateEvent(PodSpecUpdatedEvent(podId, Some(TerminalPodSpec(podId))))

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
