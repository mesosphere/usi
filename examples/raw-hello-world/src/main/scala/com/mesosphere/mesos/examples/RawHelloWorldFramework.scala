package com.mesosphere.mesos.examples

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.google.protobuf.ByteString
import com.mesosphere.mesos.client.{MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.{AgentID, Filters, Offer, OfferID, TaskID, TaskState}
import org.apache.mesos.v1.scheduler.Protos.Event.{Offers, Update}
import org.apache.mesos.v1.scheduler.Protos.{Call, Event}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Run a mesos-client example framework that:
  *  - uses only the raw mesos-client
  *  - successfully subscribes to Mesos master
  *  - starts one `echo "Hello, world" && sleep 3600` task
  *  - exits should the task fail (or fail to start)
  *
  *  Good to test against local Mesos.
  *
  */
object RawHelloWorldFramework extends App with StrictLoggingFlow {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  /**
    * Framework info
    */
  val frameworkInfo = BuilderHelper.frameworkInfo(user = "test", name = "RawHelloWorldExample").build()

  /**
    * Mesos client and its settings. We wait for the client to connect to Mesos for 10 seconds. If it can't
    * the framework will exit with a [[java.util.concurrent.TimeoutException]]
    */
  val settings = MesosClientSettings(ConfigFactory.load().getConfig("mesos-client"))
  val client = Await.result(MesosClient(settings, frameworkInfo).runWith(Sink.head), 10.seconds)

  logger.info(s"""Successfully subscribed to Mesos:
                 | Framework Id: ${client.frameworkId.getValue}
                 | Mesos host: ${client.connectionInfo.url}
       """.stripMargin)

  /**
    * This is the main framework loop:
    *
    * +--------------+
    * |              |
    * | Mesos source |
    * |              |
    * +------+-------+
    *        |
    *        |
    *        | Events (1)
    *        |
    *        v
    * +------+--------------------------+
    * |                                 |
    * |   Event Handler                 |
    * |                                 |
    * |    - Accept or decline offers   | (2)
    * |    - Acknowledge task status    |
    * |      updates                    |
    * |                                 |
    * +------+--------------------------+
    *        |
    *        |
    *        | Calls (3)
    *        |
    *        |
    * +------v-------+
    * |              |
    * |  Mesos sink  |
    * |              |
    * +--------------+
    *
    * 1. Once connected to Mesos we start receiving Mesos [[Event]]s from [[MesosClient.mesosSource]]
    *
    * 2. An event handler that currently handles two event types:
    *
    * [[Offer]]s:
    *    - if an offer have enough resources we send Mesos an [[org.apache.mesos.v1.scheduler.Protos.Call.Accept]]
    *      call with task details.
    *    - if not enough resources or no need to launch anything, we decline the offer by sending a
    *    [[org.apache.mesos.v1.scheduler.Protos.Call.Decline]]
    *
    * [[Update]]s:
    *    - If the task is happily running, we acknowledge the status update
    *    - Any other status will lead to framework termination
    *
    * 3. The above stage can produce zero or more Mesos [[Call]]s which then are sent using [[MesosClient.mesosSink]]
    *
    */
  client
    .mesosSource
    .statefulMapConcat(() => {

      // Task state. This variable is overridden when task state changes e.g. task is being launched or received
      // new task status.
      var task: Task = new Task(
        // Task specification. It uses smallest possible amount of resources and prints
        // "Hello, world" and sleeps for an hour.
        Spec(
          name = "sleep",
          cmd = """echo "Hello, world" && sleep 3600""",
          cpus = 0.1,
          mem = 32.0
        ))

      event =>
        event.getType match {
          case Event.Type.OFFERS =>
            val (newTask, calls) = handleOffers(task, event.getOffers)
            task = newTask
            calls
          case Event.Type.UPDATE =>
            val (newTask, calls) = handleUpdate(task, event.getUpdate)
            task = newTask
            calls
          case Event.Type.HEARTBEAT =>
            Nil // Nothing to do here
          case e =>
            logger.warn(s"Unhandled Mesos event: $e")
            Nil
        }
    })
    .runWith(client.mesosSink)
    .onComplete {
      case Success(res) =>
        logger.info(s"Stream completed: $res");
        system.terminate()
      case Failure(e) =>
        logger.error(s"Error in stream: $e");
        system.terminate()
    }

  def accept(task: Task, offer: Offer): Call = client.calls.newAccept(BuilderHelper.accept(task, offer).build)
  def decline(offerIds: Seq[OfferID]): Call = client.calls.newDecline(offerIds = offerIds, filters = Some(Filters.newBuilder().setRefuseSeconds(5.0).build))
  def acknowledge(agentID: AgentID, taskID: TaskID, uuid: ByteString) = client.calls.newAcknowledge(agentID, taskID, uuid)

  /**
    * Handle an [[org.apache.mesos.v1.scheduler.Protos.Event.Offers]] event. If the task hasn't been started yet -
    * match it and accept if given enough resources. Method returns a tuple with an updated [[Task]] (if there was an update)
    * and a list of Mesos [[Call]]s (accept and/or decline offer)
    */
  def handleOffers(task: Task, offers: Offers): (Task, List[Call]) = {

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
  def handleUpdate(task: Task, event: Update): (Task, List[Call]) = {
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
        val ackuuid = uuid.getOrElse(throw new IllegalArgumentException(s"Missing uuid in task status update: $event"))
        newTask -> List(acknowledge(agentID, taskID, ackuuid))

      case st =>
        throw new IllegalStateException(
          s"Received a terminal task status $st for task ${taskID.getValue}. Terminating now...")
    }
  }
}
