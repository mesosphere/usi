package com.mesosphere.usi.core.logic

import java.time.Instant

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.logic.SchedulerLogicHelpers._
import com.mesosphere.usi.core.matching.{FCFSOfferMatcher, OfferMatcher}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.protos.ProtoBuilders
import com.mesosphere.{ImplicitStrictLogging, LoggingArgs}
import org.apache.mesos.v1.Protos.{ExecutorID, ExecutorInfo}
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
      case (spec, resources) => spec.id -> buildLaunchOperation(spec, offer.getAgentId, offer.getFrameworkId, resources)
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

  private[this] def buildLaunchTaskOperation(podSpec: RunningPodSpec,
                                             agentId: Mesos.AgentID,
                                             frameworkId: Mesos.FrameworkID,
                                             taskRunTemplate: TaskRunTemplateType) = {
    import com.mesosphere.usi.core.protos.ProtoConversions._

    val taskInfoBuilder = taskRunTemplate.protoBuilder
    // TBD: Do we want to set meaningful defaults, or throw an error if it's not set?
    if (!taskInfoBuilder.hasTaskId) taskInfoBuilder.setTaskId(TaskId(podSpec.id.value).asProto)
    if (!taskInfoBuilder.hasName) taskInfoBuilder.setName(podSpec.id.value.replaceAll("[^a-zA-Z0-9-]", ""))
    taskInfoBuilder.setAgentId(agentId)

    val taskInfos = Seq(taskInfoBuilder.build())

    ProtoBuilders.newOfferOperation(
      Mesos.Offer.Operation.Type.LAUNCH,
      launch = ProtoBuilders.newOfferOperationLaunch(taskInfos))
  }

  private[this] def buildLaunchTaskGroupOperation(podSpec: RunningPodSpec,
                                                  agentId: Mesos.AgentID,
                                                  frameworkId: Mesos.FrameworkID,
                                                  taskRunTemplate: TaskGroupRunTemplateType) = {
    import com.mesosphere.usi.core.protos.ProtoConversions._

    val taskGroupInfoBuilder = taskRunTemplate.protoBuilder
    var i = 1
    taskGroupInfoBuilder.getTasksBuilderList.forEach { taskBuilder =>
      val nameAndId = podSpec.id.value + "-" + i
      // TBD: Do we want to set meaningful defaults, or throw an error if it's not set?
      if (!taskBuilder.hasTaskId) taskBuilder.setTaskId(TaskId(nameAndId).asProto)
      if (!taskBuilder.hasName) taskBuilder.setName(nameAndId.replaceAll("[^a-zA-Z0-9-]", ""))

      taskBuilder.setAgentId(agentId)
      i = i + 1
    }
    val taskGroupInfo = taskGroupInfoBuilder.build()

    val executorBuilder:ExecutorInfo.Builder = taskRunTemplate.executorBuilder
    // TBD: Do we want to set meaningful defaults, or throw an error if it's not set?
    if (!executorBuilder.hasExecutorId) executorBuilder.setExecutorId(ExecutorID.newBuilder().setValue(podSpec.id.value.replaceAll("[^a-zA-Z0-9-]", "")))
    executorBuilder.setFrameworkId(frameworkId)
    val executorInfo = executorBuilder.build()

    ProtoBuilders.newOfferOperation(
      Mesos.Offer.Operation.Type.LAUNCH_GROUP,
      launchGroup = ProtoBuilders.newOfferOperationLaunchGroup(taskGroupInfo, executorInfo))
  }

  def buildLaunchOperation(
      podSpec: RunningPodSpec,
      agentId: Mesos.AgentID,
      frameworkId: Mesos.FrameworkID,
      resources: List[Mesos.Resource]): Mesos.Offer.Operation = {

    podSpec.runSpec match {
      case templateType: TaskRunTemplateType => buildLaunchTaskOperation(podSpec, agentId, frameworkId, templateType)
      case templateType: TaskGroupRunTemplateType => buildLaunchTaskGroupOperation(podSpec, agentId, frameworkId, templateType)
    }
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
