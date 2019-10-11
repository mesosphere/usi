package com.mesosphere.usi.core.logic

import java.time.Instant

import com.mesosphere.{ImplicitStrictLogging, LoggingArgs}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core._
import com.mesosphere.usi.core.matching.{FCFSOfferMatcher, OfferMatcher}
import com.mesosphere.usi.core.models._
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

/**
  * The current home for USI Mesos event related logic
  */
private[core] class MesosEventsLogic(mesosCallFactory: MesosCalls, offerMatcher: OfferMatcher = new FCFSOfferMatcher())
    extends ImplicitStrictLogging {

  val podTaskIdStrategy: PodTaskIdStrategy = PodTaskIdStrategy.DefaultStrategy

  private[core] def matchOffer(
      offer: Mesos.Offer,
      specs: Iterable[RunningPodSpec]): (Set[PodId], SchedulerEventsBuilder) = {
    import com.mesosphere.usi.core.protos.ProtoBuilders._
    import com.mesosphere.usi.core.protos.ProtoConversions._

    val matchedSpecs: Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]] = offerMatcher.matchOffer(offer, specs)
    val launchCommands = matchedSpecs.map {
      case (spec, resourceMatches) =>
        val groupedResources = resourceMatches.groupBy(_.entityKey)
        val executorResources = groupedResources.getOrElse(None, Nil).map(_.resource)
        val taskResources = groupedResources.collect {
          case (Some(key), taskResourceMatches) =>
            key -> taskResourceMatches.map(_.resource)
        }.withDefaultValue(Nil)
        spec.id -> spec.runSpec.buildLaunchOperation(
          offer,
          CurriedPodTaskIdStrategy(spec.id, podTaskIdStrategy),
          executorResources,
          taskResources)
    }

    val eventsBuilder = launchCommands.keys.foldLeft(SchedulerEventsBuilder.empty) { (events, podId) =>
      // Add pod record for all matched pods, and remove the pod spec for the newly launched pod
      events
        .withPodRecord(podId, Some(PodRecord(podId, Instant.now(), offer.getAgentId.asModel)))
        .withPodSpec(podId, None)
    }

    val offerEvent = if (launchCommands.isEmpty) {
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
      val operations = launchCommands.values.map {
        case Right(launchGroup) =>
          logger.info(
            s"Launching TaskGroup taskId${if (launchGroup.getTaskGroup.getTasksCount > 1) "s"} : [{}] for offerId {}",
            launchGroup.getTaskGroup.getTasksList.asScala.map(_.getTaskId.getValue).mkString(", "),
            offer.getId.getValue
          )(
            LoggingArgs("offerId" -> offer.getId.getValue).and("mesosOperation" -> "LAUNCH")
          )
          newOfferOperation(Mesos.Offer.Operation.Type.LAUNCH_GROUP, launchGroup = launchGroup)
        case Left(launch) =>
          logger.info(
            s"Launching taskId${if (launch.getTaskInfosCount > 1) "s"} : [{}] for offerId {}",
            launch.getTaskInfosList.asScala.map(_.getTaskId.getValue).mkString(", "),
            offer.getId.getValue
          )(
            LoggingArgs("offerId" -> offer.getId.getValue).and("mesosOperation" -> "LAUNCH")
          )
          newOfferOperation(Mesos.Offer.Operation.Type.LAUNCH, launch = launch)
      }

      mesosCallFactory.newAccept(
        MesosCall.Accept
          .newBuilder()
          .addAllOperations(operations.asJava)
          .addOfferIds(offer.getId)
          .build()
      )
    }

    (launchCommands.keySet, eventsBuilder.withMesosCall(offerEvent))
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
        podTaskIdStrategy.unapply(taskId) match {
          case Some((podId, _)) =>
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
          case None =>
            logger.error(
              s"Critical error! Failed to derive podId from ${taskId}; associated taskStatus has been ignored!")
            SchedulerEvents.empty
        }
      case other =>
        logger.warn(s"No handler defined for event ${other.getType} - ${other}")
        SchedulerEvents.empty
    }
  }

}
