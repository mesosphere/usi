package com.mesosphere.usi.core.logic

import com.mesosphere.{ImplicitStrictLogging, LoggingArgs}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.commands.{CreateReservation, ExpungePod, KillPod, LaunchPod, SchedulerCommand}

/**
  * The current home for USI business logic for dealing with spec commands
  *
  */
private[core] class SpecLogic(mesosCallFactory: MesosCalls) extends ImplicitStrictLogging {

  private def getRunningPodSpec(podSpecs: Map[PodId, PodSpec], id: PodId): Option[RunningPodSpec] = {
    podSpecs.get(id).collect { case r: RunningPodSpec => r }
  }
  private[core] def handleCommand(state: SchedulerState)(command: SchedulerCommand): SchedulerEvents = {
    command match {
      case launchPod: LaunchPod =>
        logger.debug(s"Received launch for pod ${launchPod.podId}")(
          LoggingArgs("podId" -> launchPod.podId)
        )
        if (
          state.podRecords.contains(launchPod.podId) || getRunningPodSpec(state.podSpecs, launchPod.podId)
            .exists(_.runSpec == launchPod.runSpec)
        ) {
          // if we already have a record for the pod, ignore
          logger.debug(s"Pod ${launchPod.podId} already exists.")(
            LoggingArgs("podId" -> launchPod.podId)
          )
          SchedulerEvents.empty
        } else {
          SchedulerEvents(
            stateEvents = List(
              PodSpecUpdatedEvent(
                launchPod.podId,
                Some(RunningPodSpec(launchPod.podId, launchPod.runSpec, launchPod.domainFilter, launchPod.agentFilter))
              )
            )
          )
        }
      case ExpungePod(podId) =>
        logger.debug(s"Received expunge for pod $podId")(
          LoggingArgs("podId" -> podId)
        )
        var b = SchedulerEventsBuilder.empty
        if (state.podSpecs.contains(podId)) {
          b = b.withStateEvent(PodSpecUpdatedEvent(podId, None))
        }
        if (state.podRecords.contains(podId)) {
          b = b.withStateEvent(PodRecordUpdatedEvent(podId, None))
        }
        b.result
      case KillPod(podId) =>
        logger.debug(s"Received kill for pod $podId")(
          LoggingArgs("podId" -> podId)
        )
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
            .newKill(taskId.asProto, if (status.hasAgentId) Some(status.getAgentId) else None, None)
        )
    }
  }
}
