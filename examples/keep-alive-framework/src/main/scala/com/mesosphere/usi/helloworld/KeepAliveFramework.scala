package com.mesosphere.usi.helloworld

import java.net.URL

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.mesos.client.{CredentialsProvider, DcosServiceAccountProvider, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models._
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{TaskState, TaskStatus}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

case class KeepAliveFrameWorkSettings(clientSettings: MesosClientSettings, numberOfTasks: Int) {
  def this(config: Config) =
    this(MesosClientSettings.fromConfig(config), config.getInt("keep-alive-framework.tasks-started"))
}

class KeepAliveFramework(settings: KeepAliveFrameWorkSettings, authorization: Option[CredentialsProvider] = None)(
    implicit system: ActorSystem,
    mat: ActorMaterializer)
    extends StrictLogging {

  val client: MesosClient = new KeepAliveMesosClientFactory(settings.clientSettings, authorization).client

  val runSpec: RunTemplate = KeepAlivePodSpecHelper.runSpec

  val specsSnapshot: List[RunningPodSpec] =
    KeepAlivePodSpecHelper.specsSnapshot(settings.numberOfTasks)

  // KeepAliveWatcher looks for a terminal task and then restarts the whole pod.
  val keepAliveWatcher: Flow[StateEventOrSnapshot, SchedulerCommand, NotUsed] = Flow[StateEventOrSnapshot].mapConcat {
    // Main state event handler. We log happy events and restart the pod if something goes wrong
    case s: StateSnapshot =>
      logger.info(s"Initial state snapshot: $s")
      Nil

    case PodStatusUpdatedEvent(id, Some(PodStatus(_, taskStatuses))) =>
      import TaskState._
      def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)
      // We're only interested in the bad task statuses for our pod
      val failedTasks = taskStatuses.filterNot { case (id, status) => activeTask(status) }
      if (failedTasks.nonEmpty) {
        logger.info(s"Restarting Pod $id")
        val newId = KeepAlivePodSpecHelper.createNewIncarnationId(id)
        List(
          ExpungePod(id), // Remove the currentPod
          LaunchPod(newId, runSpec) // Launch the new pod
        )
      } else {
        Nil
      }

    case e =>
      logger.warn(s"Unhandled event: $e") // we ignore everything else for now
      Nil
  }

  val podRecordRepository = InMemoryPodRecordRepository()

  val (stateSnapshot, source, sink) =
    Await.result(Scheduler.asSourceAndSink(client, podRecordRepository, SchedulerSettings.load()), 10.seconds)

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
    .prepend(Source(specsSnapshot.map { spec =>
      LaunchPod(spec.id, spec.runSpec)
    }))
    .to(sink)
    .run()

  // We let the framework run "forever"
  io.StdIn.readLine("Keep alive framework is started")

}

object KeepAliveFramework {

  def main(args: Array[String]): Unit = {

    require(
      (2 == args.length) || (args.length == 4),
      "Please provide arguments: <dcos-url> <mesos-url> [<private-key-file> <iam-uid>]")

    implicit val system: ActorSystem = ActorSystem()
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val dcosRoot = new URL(args(0))
    val mesosUrl = new URL(args(1))
    val provider = if (args.length == 3) {
      val privateKey = scala.io.Source.fromFile(args(2)).mkString
      Some(DcosServiceAccountProvider(args(3), privateKey, dcosRoot))
    } else {
      None
    }

    val conf = ConfigFactory.load().getConfig("mesos-client").withFallback(ConfigFactory.load())
    val settings = KeepAliveFrameWorkSettings(
      MesosClientSettings.fromConfig(conf).withMasters(Seq(mesosUrl)),
      conf.getInt("keep-alive-framework.tasks-started"))
    new KeepAliveFramework(settings, provider)
  }

  def apply(conf: Config)(implicit system: ActorSystem, mat: ActorMaterializer): KeepAliveFramework =
    new KeepAliveFramework(new KeepAliveFrameWorkSettings(conf))
}
