package com.mesosphere.usi.helloworld

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{TaskState, TaskStatus}

class KeepAliveFramework(conf: Config) extends StrictLogging {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val client = new KeepAliveMesosClientFactory(conf).client

  val runSpec = KeepAlivePodSpecHelper.runSpec

  val specsSnapshot = KeepAlivePodSpecHelper.specsSnapshot(conf.getInt("keep-alive-framework.tasks-started"))

  // KeepAliveWatcher looks for a terminal task and then restarts the whole pod.
  val keepAliveWatcher: Flow[StateEvent, SpecUpdated, NotUsed] = Flow[StateEvent].mapConcat {
    // Main state event handler. We log happy events and restart the pod if something goes wrong
    case s: StateSnapshot =>
      logger.info(s"Initial state snapshot: $s")
      Nil

    case PodStatusUpdated(id, Some(PodStatus(_, taskStatuses))) =>
      import TaskState._
      def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)
      // We're only interested in the bad task statuses for our pod
      val failedTasks = taskStatuses.filterNot { case (id, status) => activeTask(status) }
      if (failedTasks.nonEmpty) {
        logger.info(s"Restarting Pod $id")
        val newId = KeepAlivePodSpecHelper.createNewIncarnationId(id)
        List(
          PodSpecUpdated(id, None), // Remove the currentPod
          PodSpecUpdated(newId, Some(PodSpec(newId, Goal.Running, runSpec))) // Launch the new pod
        )
      } else {
        Nil
      }

    case e =>
      logger.warn(s"Unhandled event: $e") // we ignore everything else for now
      Nil
  }

  val (stateSnapshot, source, sink) = Scheduler.asSourceAndSink(specsSnapshot, client)

  /**
    * This is the core part of this framework. Source with SpecEvents is pushing events to the keepAliveWatcher,
    * which pushes the pod updates to the SpecUpdates Sink:

      +-----------------------+
      |SpecEvents Source      |
      |(What happened to pod) |
      +----------+------------+
                 |
                 v
      +----------+------------+
      |keepAliveWatcher       |
      |(listens to PodFinished|
      |events and issues      |
      |commands to spawn new  |
      |pods)                  |
      +----------+------------+
                 |
                 v
      +----------+------------+
      |SpecUpdates Sink       |
      |(commands to launch    |
      |new pods)              |
      +-----------------------+

    */
  source
    .via(keepAliveWatcher)
    .to(sink)
    .run()

  // We let the framework run "forever"
  io.StdIn.readLine("Keep alive framework is started")

}

object KeepAliveFramework {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("mesos-client").withFallback(ConfigFactory.load())
    KeepAliveFramework(conf)
  }

  def apply(conf: Config): KeepAliveFramework = new KeepAliveFramework(conf)
}
