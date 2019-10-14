package com.mesosphere.usi.helloworld

import java.io.File
import java.net.URI

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.mesos.client.{CredentialsProvider, DcosServiceAccountProvider, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.{commands, _}
import com.mesosphere.usi.core.models.commands.{ExpungePod, LaunchPod, SchedulerCommand}
import com.mesosphere.usi.core.models.template.RunTemplate
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{TaskState, TaskStatus}
import scopt.OParser

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

case class KeepAliveFrameWorkSettings(clientSettings: MesosClientSettings, numberOfTasks: Int, role: String)

class KeepAliveFramework(settings: KeepAliveFrameWorkSettings, authorization: Option[CredentialsProvider] = None)(
    implicit system: ActorSystem,
    mat: ActorMaterializer)
    extends StrictLogging {

  val client: MesosClient =
    new KeepAliveMesosClientFactory(settings.clientSettings, authorization, settings.role).client

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
  val end = source
    .via(keepAliveWatcher)
    .prepend(Source(specsSnapshot.map { spec =>
      commands.LaunchPod(spec.id, spec.runSpec)
    }))
    .runWith(sink)

  // We let the framework run "forever"
  val result = Await.result(end, Duration.Inf)
  logger.warn(s"Framework finished with $result")
}

object KeepAliveFramework {

  case class Args(
      dcosRoot: URI = new URI("http://localhost"),
      mesosUrl: URI = new URI("http://localhost:5050"),
      dcosCertPath: Option[String] = None,
      privateKey: Option[File] = None,
      iamUid: Option[String] = None,
      mesosRole: String = "test"
  )
  val argsParser = {
    val builder = OParser.builder[Args]
    import builder._

    OParser.sequence(
      programName("keep-alive-framework"),
      head("keep-alive-framework"),
      opt[URI]("dcos-url")
        .action((root, c) => c.copy(dcosRoot = root))
        .text("The DC/OS root address."),
      opt[URI]("mesos-url")
        .action((mesos, c) => c.copy(mesosUrl = mesos))
        .text("The Mesos master URL."),
      opt[String]("dcos-ca.cert")
        .optional()
        .action((cert, c) => c.copy(dcosCertPath = Some(cert)))
        .text("Optional path to a DC/OS SSL certificate."),
      opt[File]("private-key-file")
        .optional()
        .action((key, c) => c.copy(privateKey = Some(key)))
        .text("Optional private key for strict authentication."),
      opt[String]("iam-uid")
        .optional()
        .action((iam, c) => c.copy(iamUid = Some(iam)))
        .text("Optional DC/OS IAM user id."),
      opt[String]("mesos-role")
        .optional()
        .action((role, c) => c.copy(mesosRole = role))
        .text("Optional Mesos role. Defaults to 'test'."),
      checkConfig { c =>
        if ((c.privateKey.isEmpty && c.iamUid.nonEmpty) || (c.privateKey.nonEmpty && c.iamUid.isEmpty)) {
          failure("Private key and IAM user must defined together.")
        } else {
          success
        }
      }
    )
  }

  /**
    * Set ups the same way as [[MesosClientExampleFramework.main]] and run with
    * {{{
    *   ./gradlew :keep-alive-framework:run --stacktrace --args "\
    *     --dcos-url $(dcos config show core.dcos_url) \
    *     --mesos-url $(dcos config show core.dcos_url)/mesos \
    *     --dcos-ca.cert $(pwd)/dcos-ca.crt \
    *     --private-key-file $(pwd)/usi.private.pem --iam-uid strict-usi\
    *     --mesos-role usi"
    * }}}
    */
  def main(args: Array[String]): Unit = {

    OParser.parse(argsParser, args, Args()) match {
      case Some(Args(dcosRoot, mesosUrl, maybeDcosCertPath, privateKey, iamUid, mesosRole)) =>
        val akkaConfig: Config = maybeDcosCertPath match {
          case Some(dcosCertPath) =>
            ConfigFactory.parseString(s"""
              |akka.ssl-config.trustManager.stores = [
              | { type: "PEM", path: $dcosCertPath }
              |]
              """.stripMargin).withFallback(ConfigFactory.load())
          case None => ConfigFactory.load()
        }

        implicit val system: ActorSystem = ActorSystem("keep-alive", akkaConfig)
        implicit val mat: ActorMaterializer = ActorMaterializer()
        implicit val ec: ExecutionContextExecutor = system.dispatcher

        val provider = (privateKey, iamUid) match {
          case (Some(privateKeyFile), Some(iam)) =>
            val privateKey = scala.io.Source.fromFile(privateKeyFile).mkString
            Some(DcosServiceAccountProvider(iam, privateKey, dcosRoot.toURL))
          case _ => None
        }

        val conf = ConfigFactory.load().getConfig("mesos-client").withFallback(ConfigFactory.load())
        val settings = KeepAliveFrameWorkSettings(
          MesosClientSettings.fromConfig(conf).withMasters(Seq(mesosUrl.toURL)),
          conf.getInt("keep-alive-framework.tasks-started"),
          mesosRole)
        new KeepAliveFramework(settings, provider)
      case _ =>
        sys.exit(1)
    }
  }
}
