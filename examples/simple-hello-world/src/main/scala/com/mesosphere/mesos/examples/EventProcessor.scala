package com.mesosphere.mesos.examples
import com.google.protobuf.ByteString
import com.mesosphere.mesos.client.MesosCalls
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{Filters, Offer, OfferID, TaskState}
import org.apache.mesos.v1.scheduler.Protos.Event.{Offers, Update}
import org.apache.mesos.v1.scheduler.Protos.{Call, Event}

import scala.collection.JavaConverters._

class EventProcessor(mesosCalls: MesosCalls) extends StrictLogging {

  /**
    * Main entry point to process all Mesos [[Event]]s. It receives current framework state (which in this case is only
    * one task) and current event and responds with a tuple: a new [[Task]] which represents the new (and possibly
    * updated state) and a list of Mesos [[Call]]s to send to Mesos.
    *
    * @param task existing task state
    * @param event Mesos event
    * @return
    */
  def process(task: Task, event: Event): (Task, List[Call]) = {
    event.getType match {
      case Event.Type.OFFERS => processOffers(task, event.getOffers)
      case Event.Type.UPDATE => processUpdate(task, event.getUpdate)
      case Event.Type.HEARTBEAT => task -> Nil
      case e => logger.warn(s"Unhandled Mesos event: $e"); task -> Nil
    }
  }

  /**
    * Handle an [[org.apache.mesos.v1.scheduler.Protos.Event.Offers]] event. If the task hasn't been started yet -
    * match it and accept if given enough resources. Method returns a tuple with an updated [[Task]] (if there was an update)
    * and a list of Mesos [[Call]]s (accept and/or decline offer)
    */
  private[this] def processOffers(task: Task, offers: Offers): (Task, List[Call]) = {

    def accept(task: Task, offer: Offer): Call = mesosCalls.newAccept(ProtosHelper.accept(task, offer).build)
    def decline(offerIds: Seq[OfferID]): Call =
      mesosCalls.newDecline(offerIds = offerIds, filters = Some(Filters.newBuilder().setRefuseSeconds(5.0).build))

    val offersList = offers.getOffersList.asScala
    val offerIds = offersList.map(_.getId)

    if (task.isScheduled) {
      OfferMatcher.matchOffer(task.spec, offersList) match {
        case Some(offer) =>
          logger.info(s"Found a matching offer: ${offer.getId.getValue}. Launching the task ${task.taskId}")
          val newTask = task.copy(agentId = Some(offer.getAgentId))
          newTask -> List(
            accept(newTask, offer), // we accept the matching offer
            decline(offerIds.filter(_ != offer.getId)) // and decline all the others
          )
        case None =>
          logger.info(s"No matching offers found. Declining offer[s]: $offerIds")
          task -> List(decline(offerIds))
      }
    } else {
      logger.info("No scheduled tasks found. Declining all offers")
      task -> List(decline(offerIds))
    }
  }

  /**
    * Handle an [[org.apache.mesos.v1.scheduler.Protos.Event.Update]] event. Most status updates are simply logged
    * but on any terminal status update (e.g. [[org.apache.mesos.v1.Protos.TaskState.TASK_FINISHED]]) the framework
    * will exit with an exitCode > 0.  Method returns a tuple with new [[Task]] state and a list of Mesos [[Call]]s
    * (task status acknowledgement).
    */
  private[this] def processUpdate(task: Task, event: Update): (Task, List[Call]) = {

    val s = event.getStatus

    val uuid: Option[ByteString] = if (s.hasUuid) Some(s.getUuid) else None
    val agentID = s.getAgentId
    val taskID = s.getTaskId
    val taskState = s.getState

    assert(
      taskID.getValue == task.taskId,
      s"We received a status update for task Id ${taskID.getValue} but our task Id is ${task.taskId}! Goodbye cruel world (╯°□°）╯︵ ┻━┻"
    )

    import TaskState._
    taskState match {
      // We handle only "happy" task states by acknowledging them and exit on everything else
      case TASK_RUNNING | TASK_STAGING | TASK_STARTING =>
        // Only task status updates that have UUIDs must be acknowledged.
        // This is always the case for status updates
        val newTask = task.copy(state = Some(taskState))
        val msgUuid = uuid.getOrElse(throw new IllegalArgumentException(s"Missing uuid in task status update: $event"))
        newTask -> List(mesosCalls.newAcknowledge(agentID, taskID, msgUuid))

      case st =>
        throw new IllegalStateException(
          s"Received a terminal task status $st for task ${taskID.getValue}. Terminating now...")
    }
  }
}
