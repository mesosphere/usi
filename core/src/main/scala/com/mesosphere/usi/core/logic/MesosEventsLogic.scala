package com.mesosphere.usi.core.logic

import java.time.Instant

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.models._
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * The current home for USI Mesos event related logic
  */
private[core] class MesosEventsLogic(mesosCallFactory: MesosCalls) extends StrictLogging {
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
                name = "hi",
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

  private def matchOffer(offer: Mesos.Offer, specs: Iterable[PodSpec]): (Set[PodId], SchedulerEventsBuilder) = {
    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._
    val groupedResources = offer.getResourcesList.asScala.groupBy { r =>
      ResourceType.fromName(r.getName)
    }
    val taskInfos = matchPodSpecsTaskRecords(offer, groupedResources, Map.empty, specs.toList)
    val intentsBuilder = taskInfos.keys.foldLeft(SchedulerEventsBuilder.empty) { (intents, podId) =>
      intents.withPodRecord(podId, Some(PodRecord(podId, Instant.now(), offer.getAgentId.asModel)))
    }

    val op =
      newOfferOperation(Mesos.Offer.Operation.Type.LAUNCH, launch = newOfferOperationLaunch(taskInfos.values.flatten))

    val acceptBuilder = MesosCall.Accept
      .newBuilder()
      .addOperations(op)
      .addOfferIds(offer.getId)

    val intents = intentsBuilder.withMesosCall(mesosCallFactory.newAccept(acceptBuilder.build))

    (taskInfos.keySet, intents)
  }

  def processEvent(specs: SpecState, state: SchedulerState, pendingLaunch: Set[PodId])(
      event: MesosEvent): SchedulerEvents = {
    import com.mesosphere.usi.core.protos.ProtoConversions.EventMatchers._
    event match {
      case OffersEvent(offersList) =>
        val (schedulerEventsBuilder, _) =
          offersList.asScala.foldLeft((SchedulerEventsBuilder.empty, pendingLaunch)) {
            case ((builder, pending), offer) =>
              val (matchedPodIds, offerMatchSchedulerEvents) = matchOffer(offer, pendingLaunch.flatMap { podId =>
                specs.podSpecs.get(podId)
              }(collection.breakOut))

              (builder ++ offerMatchSchedulerEvents, pending -- matchedPodIds)
          }
        schedulerEventsBuilder.result

      case UpdateEvent(taskStatus) =>
        val taskId = TaskId(taskStatus.getTaskId.getValue)
        val podId = podIdFor(taskId)

        if (specs.podSpecs.contains(podId)) {
          val newState = state.podStatuses.get(podId) match {
            case Some(status) =>
              status.copy(taskStatuses = status.taskStatuses.updated(taskId, taskStatus))
            case None =>
              PodStatus(podId, Map(taskId -> taskStatus))
          }

          SchedulerEvents(stateEvents = List(PodStatusUpdated(podId, Some(newState))))

        } else {
          SchedulerEvents.empty
        }
      case other =>
        logger.warn(s"No handler defined for event ${other.getType} - ${other}")
        SchedulerEvents.empty
    }
  }

}
