package com.mesosphere.usi.core.logic

import java.time.Instant

import com.mesosphere.{ImplicitStrictLogging, LoggingArgs}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.matching.{FCFSOfferMatcher, OfferMatcher}
import com.mesosphere.usi.core.models._
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}
import SchedulerLogicHelpers._

import scala.collection.JavaConverters._

/**
  * The current home for USI Mesos event related logic
  */
private[core] class MesosEventsLogic(mesosCallFactory: MesosCalls, offerMatcher: OfferMatcher = new FCFSOfferMatcher())
    extends ImplicitStrictLogging {

  private[core] def matchOffer(
      offer: Mesos.Offer,
      specs: Iterable[RunningPodSpec]): (Set[PodId], SchedulerEventsBuilder) = {
    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._

    val matchedSpecs: Map[RunningPodSpec, scala.List[Mesos.Resource]] = offerMatcher.matchOffer(offer, specs)
    val taskInfos = matchedSpecs.map {
      case (spec, resources) => spec.id -> buildTaskInfos(spec, offer.getAgentId, resources)
    }

    val eventsBuilder = taskInfos.keys.foldLeft(SchedulerEventsBuilder.empty) { (events, podId) =>
      // Add pod record for all matched pods, and remove the pod spec for the newly launched pod
      events
        .withPodRecord(podId, Some(PodRecord(podId, Instant.now(), offer.getAgentId.asModel)))
        .withPodSpec(podId, None)
    }

    val offerEvent = if (taskInfos.isEmpty) {
      logger.info(
        s"Declining offer with id [{}] {}",
        offer.getId.getValue,
        if (specs.isEmpty) "as there are no specs to be launched"
        else s"due to unmet requirement for pods : [${specs.map(_.id.value).mkString(", ")}]"
      )(
        LoggingArgs("offerId" -> offer.getId.getValue).and("mesosOperation" -> "DECLINE")
      )
      mesosCallFactory.newDecline(Seq(offer.getId))
    } else {
      val op = newOfferOperation(
        Mesos.Offer.Operation.Type.LAUNCH,
        launch = newOfferOperationLaunch(taskInfos.values.flatten)
      )
      logger.info(
        s"Launching taskId${if (op.getLaunch.getTaskInfosCount > 1) "s"} : [{}] for offerId {}",
        op.getLaunch.getTaskInfosList.asScala.map(_.getTaskId.getValue).mkString(", "),
        offer.getId.getValue
      )(
        LoggingArgs("offerId" -> offer.getId.getValue).and("mesosOperation" -> "LAUNCH")
      )
      mesosCallFactory.newAccept(
        MesosCall.Accept
          .newBuilder()
          .addOperations(op)
          .addOfferIds(offer.getId)
          .build()
      )
    }

    (taskInfos.keySet, eventsBuilder.withMesosCall(offerEvent))
  }

  /**
    * Given a [[RunningPodSpec]], an [[Mesos.AgentID]] and a list of matched resources return a list of [[Mesos.TaskInfo]]s
    *
    * @param podSpec podSpec
    * @param agentId agentId from the offer
    * @param resources list of matched resources
    * @return
    */
  def buildTaskInfos(
      podSpec: RunningPodSpec,
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
        command = newCommandInfo(podSpec.runSpec.shellCommand, podSpec.runSpec.fetch),
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
        var b = SchedulerEventsBuilder.empty

        val taskId = TaskId(taskStatus.getTaskId.getValue)

        if (taskStatus.hasUuid) {
          // frameworks should accept only status updates that have UUID set
          // http://mesos.apache.org/documentation/latest/scheduler-http-api/#acknowledge
          b = b.withMesosCall(
            mesosCallFactory.newAcknowledge(taskStatus.getAgentId, taskStatus.getTaskId, taskStatus.getUuid))
        }

        val podId = podIdFor(taskId)
        logger.info(
          s"Received task status update from taskId $taskId and podId $podId with status ${taskStatus.getState}"
        )(LoggingArgs("taskId" -> taskId, "podId" -> podId))

        val alreadySet = state.podStatuses
          .get(podId)
          .flatMap { podStatus =>
            podStatus.taskStatuses.get(taskId)
          }
          .exists(_.getUuid == taskStatus.getUuid)

        if (!alreadySet) {
          val newPodStatus = state.podStatuses.get(podId) match {
            case Some(oldPodStatus) =>
              oldPodStatus.copy(taskStatuses = oldPodStatus.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }
          b = b.withPodStatus(podId, Some(newPodStatus))
        }

        b.result
      case other =>
        logger.warn(s"No handler defined for event ${other.getType} - ${other}")
        SchedulerEvents.empty
    }
  }

}
