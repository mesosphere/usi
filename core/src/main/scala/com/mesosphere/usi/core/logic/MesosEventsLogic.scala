package com.mesosphere.usi.core.logic

import java.time.Instant

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.logic.SchedulerLogicHelpers._
import com.mesosphere.usi.core.matching.{FCFSOfferMatcher, OfferMatcher}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.protos.ProtoBuilders
import com.mesosphere.{ImplicitStrictLogging, LoggingArgs}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

/**
  * The current home for USI Mesos event related logic
  */
private[core] class MesosEventsLogic(mesosCallFactory: MesosCalls, offerMatcher: OfferMatcher = new FCFSOfferMatcher())
    extends ImplicitStrictLogging {

  private val MDC_MESOS_OP = "mesosOperation"
  private val MDC_OFFER_ID = "offerId"

  private[core] def matchOffer(
      offer: Mesos.Offer,
      specs: Iterable[RunningPodSpec]): (Set[PodId], SchedulerEventsBuilder) = {
    import com.mesosphere.usi.core.protos.ProtoConversions._

    val matchedSpecs: Map[RunningPodSpec, scala.List[Mesos.Resource]] = offerMatcher.matchOffer(offer, specs)
    val launchOps = matchedSpecs.map {
      case (spec, resources) =>
        spec.id -> buildLaunchOperation(spec, offer.getAgentId, resources) //buildTaskInfos(spec, offer.getAgentId, resources)
    }

    val eventsBuilder = launchOps.keys.foldLeft(SchedulerEventsBuilder.empty) { (events, podId) =>
      // Add pod record for all matched pods, and remove the pod spec for the newly launched pod
      events
        .withPodRecord(podId, Some(PodRecord(podId, Instant.now(), offer.getAgentId.asModel)))
        .withPodSpec(podId, None)
    }

    val offerEvent = if (launchOps.isEmpty) {
      logger.info(
        s"Declining offer with id [{}] {}",
        offer.getId.getValue,
        if (specs.isEmpty) "as there are no specs to be launched"
        else s"due to unmet requirement for pods : [${specs.map(_.id.value).mkString(", ")}]"
      )(
        LoggingArgs(MDC_OFFER_ID -> offer.getId.getValue).and(MDC_MESOS_OP -> "DECLINE")
      )
      mesosCallFactory.newDecline(Seq(offer.getId))
    } else {
      launchOps.values.foreach(launchOp => {
        logger.info(
          "Launching taskIds: [{}] for offerId {}",
          launchOp.getLaunch.getTaskInfosList.asScala.map(_.getTaskId.getValue).mkString(", "),
          offer.getId.getValue)
        LoggingArgs(MDC_OFFER_ID -> offer.getId.getValue).and(MDC_MESOS_OP -> "LAUNCH")
      })

      mesosCallFactory.newAccept(
        MesosCall.Accept
          .newBuilder()
          .addAllOperations(launchOps.values.asJava)
          .addOfferIds(offer.getId)
          .build()
      )
    }

    (launchOps.keySet, eventsBuilder.withMesosCall(offerEvent))
  }

  def buildLaunchOperation(
      podSpec: RunningPodSpec,
      agentId: Mesos.AgentID,
      resources: List[Mesos.Resource]): Mesos.Offer.Operation = {

    podSpec.runSpec match {
      case runTemplate: SimpleRunTemplate =>
        val taskInfo = buildTaskInfosSimple(podSpec, runTemplate, agentId, resources)
        ProtoBuilders.newOfferOperation(
          Mesos.Offer.Operation.Type.LAUNCH,
          launch = ProtoBuilders.newOfferOperationLaunch(taskInfo))
      case runTemplate: TaskRunTemplate =>
        val taskInfo = buildTaskInfosApp(runTemplate, agentId)
        ProtoBuilders.newOfferOperation(
          Mesos.Offer.Operation.Type.LAUNCH,
          launch = ProtoBuilders.newOfferOperationLaunch(taskInfo))
      case runTemplate: TaskGroupRunTemplate =>
        val taskGroupInfo = buildTaskInfosPod(runTemplate, agentId)
        ProtoBuilders.newOfferOperation(
          Mesos.Offer.Operation.Type.LAUNCH,
          launchGroup = ProtoBuilders.newOfferOperationLaunchGroup(taskGroupInfo))
    }
  }

  private[this] def buildTaskInfosApp(runTemplate: TaskRunTemplate, agentId: Mesos.AgentID): List[Mesos.TaskInfo] = {
    List(
      runTemplate.task.toBuilder
        .setAgentId(agentId)
        .build()
    )
  }

  private[this] def buildTaskInfosPod(
      runTemplate: TaskGroupRunTemplate,
      agentId: Mesos.AgentID): Mesos.TaskGroupInfo = {

    val tgBuilder = runTemplate.taskGroup.toBuilder

    tgBuilder.getTasksBuilderList.forEach(taskBuilder => taskBuilder.setAgentId(agentId))

    tgBuilder.build()
  }

  private[this] def buildTaskInfosSimple(
      podSpec: RunningPodSpec,
      runTemplate: SimpleRunTemplate,
      agentId: Mesos.AgentID,
      resources: List[Mesos.Resource]): List[Mesos.TaskInfo] = {
    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._

    // Note - right now, runSpec only describes a single task. This needs to be improved in the future.
    taskIdsFor(podSpec).map { taskId =>
      newTaskInfo(
        taskId.asProto,
        // we use sanitized podId as the task name for now
        name = podSpec.id.value.replaceAll("[^a-zA-Z0-9-]", ""),
        agentId = agentId,
        command = newCommandInfo(runTemplate.shellCommand, runTemplate.fetch),
        resources = resources
      )
    }(collection.breakOut)
  }

  def processEvent(state: SchedulerState)(event: MesosEvent): SchedulerEvents = {
    import com.mesosphere.usi.core.protos.ProtoConversions.EventMatchers._
    event match {
      case OffersEvent(offersList) =>
        val pendingLaunchPodSpecs: Map[PodId, RunningPodSpec] =
          state.podSpecs.collect { case (id, runningPodSpec: RunningPodSpec) => id -> runningPodSpec }

        val (schedulerEventsBuilder, _) =
          offersList.asScala.foldLeft((SchedulerEventsBuilder.empty, pendingLaunchPodSpecs)) {
            case ((builder, pending), offer) =>
              val (matchedPodIds, offerMatchSchedulerEvents) = matchOffer(
                offer,
                pending.values
              )

              (builder ++ offerMatchSchedulerEvents, pending -- matchedPodIds)
          }
        schedulerEventsBuilder.result

      case UpdateEvent(taskStatus) =>
        val taskId = TaskId(taskStatus.getTaskId.getValue)
        val podId = podIdFor(taskId)
        logger.info(
          s"Received task status update from taskId $taskId and podId $podId with status ${taskStatus.getState}"
        )(LoggingArgs("taskId" -> taskId, "podId" -> podId))

        val newStatus = state.podStatuses.get(podId) match {
          case Some(oldStatus) =>
            oldStatus.copy(taskStatuses = oldStatus.taskStatuses.updated(taskId, taskStatus))
          case None =>
            PodStatus(podId, Map(taskId -> taskStatus))
        }

        SchedulerEvents(
          stateEvents = List(PodStatusUpdatedEvent(podId, Some(newStatus))),
          mesosCalls = if (taskStatus.hasUuid) {
            // frameworks should accept only status updates that have UUID set
            // http://mesos.apache.org/documentation/latest/scheduler-http-api/#acknowledge
            List(mesosCallFactory.newAcknowledge(taskStatus.getAgentId, taskStatus.getTaskId, taskStatus.getUuid))
          } else {
            Nil
          }
        )
      case other =>
        logger.warn(s"No handler defined for event ${other.getType} - ${other}")
        SchedulerEvents.empty
    }
  }

}
