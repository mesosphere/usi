package com.mesosphere.usi.core.logic

import com.mesosphere.ImplicitStrictLogging
import com.mesosphere.LoggingArgs
import java.time.Instant
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * The current home for USI Mesos event related logic
  */
private[core] class MesosEventsLogic(mesosCallFactory: MesosCalls) extends ImplicitStrictLogging {
  import SchedulerLogicHelpers._
  private case class ResourceMatch(podSpec: PodSpec, resources: Seq[Mesos.Resource])
  @tailrec private def maybeMatchPodSpec(
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      matchedResources: List[Mesos.Resource],
      resourceRequirements: List[ResourceRequirement])
    : Option[(List[Mesos.Resource], Map[ResourceType, Seq[Mesos.Resource]])] = {
    resourceRequirements match {
      case Nil =>
        Some((matchedResources, remainingResources))
      case req :: rest =>
        req.matchAndConsume(remainingResources.getOrElse(req.resourceType, Nil)) match {
          case Some(matchResult) =>
            maybeMatchPodSpec(
              remainingResources.updated(req.resourceType, matchResult.remainingResource),
              matchResult.matchedResources.toList ++ matchedResources,
              rest)
          case None =>
            // we didn't match
            None
        }
    }
  }

  @tailrec private def matchPodSpecsTaskRecords(
      offer: Mesos.Offer,
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      result: Map[PodId, List[Mesos.TaskInfo]],
      pendingLaunchPodSpecs: List[PodSpec]): Map[PodId, List[Mesos.TaskInfo]] = {

    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._

    pendingLaunchPodSpecs match {
      case Nil =>
        result

      case podSpec :: rest =>
        maybeMatchPodSpec(remainingResources, Nil, podSpec.runSpec.resourceRequirements.toList) match {
          case Some((matchedResources, newRemainingResources)) =>
            // Note - right now, runSpec only describes a single task. This needs to be improved in the future.
            val taskInfos: List[Mesos.TaskInfo] = taskIdsFor(podSpec).map { taskId =>
              newTaskInfo(
                taskId.asProto,
                // we use sanitized podId as the task name for now
                name = podSpec.id.value.replaceAll("[^a-zA-Z0-9-]", ""),
                agentId = offer.getAgentId,
                command = Mesos.CommandInfo
                  .newBuilder()
                  .setShell(true)
                  .setValue(podSpec.runSpec.shellCommand)
                  .build,
                resources = matchedResources
              )
            }(collection.breakOut)

            matchPodSpecsTaskRecords(offer, newRemainingResources, result.updated(podSpec.id, taskInfos), rest)
          case None =>
            matchPodSpecsTaskRecords(offer, remainingResources, result, rest)
        }
    }
  }

  private[core] def matchOffer(offer: Mesos.Offer, specs: Iterable[PodSpec]): (Set[PodId], SchedulerEventsBuilder) = {
    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._
    val groupedResources = offer.getResourcesList.asScala.groupBy { r =>
      ResourceType.fromName(r.getName)
    }
    val taskInfos = matchPodSpecsTaskRecords(offer, groupedResources, Map.empty, specs.toList)
    val eventsBuilder = taskInfos.keys.foldLeft(SchedulerEventsBuilder.empty) { (events, podId) =>
      events.withPodRecord(podId, Some(PodRecord(podId, Instant.now(), offer.getAgentId.asModel)))
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

  def processEvent(specs: SpecState, state: SchedulerState, pendingLaunch: Set[PodId])(
      event: MesosEvent): SchedulerEvents = {
    import com.mesosphere.usi.core.protos.ProtoConversions.EventMatchers._
    event match {
      case OffersEvent(offersList) =>
        val (schedulerEventsBuilder, _) =
          offersList.asScala.foldLeft((SchedulerEventsBuilder.empty, pendingLaunch)) {
            case ((builder, pending), offer) =>
              val (matchedPodIds, offerMatchSchedulerEvents) = matchOffer(
                offer,
                pendingLaunch.flatMap(specs.podSpecs.get)
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

        if (specs.podSpecs.contains(podId)) {
          val newStatus = state.podStatuses.get(podId) match {
            case Some(oldStatus) =>
              oldStatus.copy(taskStatuses = oldStatus.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          SchedulerEvents(
            stateEvents = List(PodStatusUpdated(podId, Some(newStatus))),
            mesosCalls = if (taskStatus.hasUuid) {
              // frameworks should accept only status updates that have UUID set
              List(mesosCallFactory.newAcknowledge(taskStatus.getAgentId, taskStatus.getTaskId, taskStatus.getUuid))
            } else {
              Nil
            }
          )

        } else {
          SchedulerEvents.empty
        }
      case other =>
        logger.warn(s"No handler defined for event ${other.getType} - ${other}")
        SchedulerEvents.empty
    }
  }

}
