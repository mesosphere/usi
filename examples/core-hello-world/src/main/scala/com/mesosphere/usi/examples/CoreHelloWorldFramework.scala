package com.mesosphere.usi.examples

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitch, Materializer}
import akka.{Done, NotUsed}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.SchedulerFactory
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.models.commands.{LaunchPod, SchedulerCommand}
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory
import com.mesosphere.usi.core.models.{PodId, PodStatus, PodStatusUpdatedEvent, StateEvent, StateSnapshot, commands}
import com.mesosphere.usi.repository.PodRecordRepository
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{FrameworkID, FrameworkInfo, TaskState, TaskStatus}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.SystemProperties
import scala.util.{Failure, Success}

/**
  * Run the hello-world example framework that:
  *  - relies on our `core` module to handle Mesos connection, event handling, offer matching etc. as
  * opposed to the `simple-hello-world` framework which uses only the Mesos client and has to implement
  * all of the above mentioned pieces itself
  *  - starts one `echo "Hello, world" && sleep N` task
  *  - exits when the task fails (or finishes)
  *
  * Good to test against local Mesos.
  *
  */
case class CoreHelloWorldFramework(frameworkId: FrameworkID, killSwitch: KillSwitch, result: Future[Done])

object CoreHelloWorldFramework extends StrictLogging {

  /**
    * Scheduler Flow might look scary at the first glance (and sometimes even at second) but it is
    * at the core (!) of any framework. Basically it is a Flow that [[SchedulerCommand]] events as input and produces
    * [[com.mesosphere.usi.core.models.StateEventOrSnapshot]] as an output.
    *
    * +--------------------+         +-----------------------+        +-----------------------+
    * |                    |         |                       |        |                       |
    * |  SchedulerCommand  |         |                       |        |      StateEvent       |
    * |                    +--------->  Core SchedulerFlow   +-------->                       |
    * |   launch and kill  |         |                       |        | replicated Pod, Agent |
    * |        pods        |         |                       |        | and Reservations state|
    * +--------------------+         +-----------------------+        +-----------------------+
    *
    *
    * In a nutshell: frameworks sends [[SchedulerCommand]] commands down the flow and receives
    * [[com.mesosphere.usi.core.models.StateEventOrSnapshot]]s when things change. USI remembers which pods it has launched by the
    * use of PodRecords. These are persisted and used to trigger an initial [task
    * reconciliation](http://mesos.apache.org/documentation/latest/reconciliation/) by the scheduler.
    *
    * For the output parameter of the scheduler Flow - it's a tuple of [[com.mesosphere.usi.core.models.StateSnapshot]]
    * and a Source of [[com.mesosphere.usi.core.models.StateEventOrSnapshot]], which means that the first state event received by
    * the framework will be a snapshot of all reconciled tasks states, followed by a individual updates.
    *
    * For more about the scheduler flow see [[Scheduler]] class documentation.
    *
    */
  def main(args: Array[String]): Unit = {
    implicit val actorSystem: ActorSystem = ActorSystem()
    implicit val ec: ExecutionContext = actorSystem.dispatcher
    val settings = MesosClientSettings.load()
    try {
      run(settings).result.onComplete {
        case Success(res) =>
          logger.info(s"Stream completed: $res");
          actorSystem.terminate()
        case Failure(e) =>
          logger.error(s"Error in stream: $e");
          actorSystem.terminate()
      }
    } catch {
      case ex: Throwable =>
        System.err.println(s"Exception while starting framework! ${ex}")
        ex.printStackTrace(System.err)
        actorSystem.terminate()
        System.exit(1)
    }
  }

  def buildFrameworkInfo: FrameworkInfo = {
    FrameworkInfo
      .newBuilder()
      .setUser(
        new SystemProperties()
          .get("user.name")
          .getOrElse(throw new IllegalArgumentException("A local user is needed to launch Mesos tasks")))
      .setName("CoreHelloWorldExample")
      .addRoles("test")
      .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
      .setFailoverTimeout(0d)
      .build()
  }

  def generateLaunchCommand: LaunchPod = {
    // Lets construct the initial specs snapshot which will contain our hello-world PodSpec. For that we generate
    // - a unique PodId
    // - a RunSpec with minimal resource requirements and hello-world shell command
    // - a snapshot containing our PodSpec
    val podId = PodId(s"hello-world.${UUID.randomUUID()}")
    val runSpec = SimpleRunTemplateFactory(
      resourceRequirements = List(ScalarRequirement.cpus(0.1), ScalarRequirement.memory(32)),
      shellCommand = """echo "Hello, world" && sleep 123456789""",
      role = "test"
    )
    commands.LaunchPod(podId, runSpec = runSpec)
  }

  def init(
      clientSettings: MesosClientSettings,
      podRecordRepository: PodRecordRepository,
      frameworkInfo: FrameworkInfo
  )(
      implicit system: ActorSystem,
      materializer: Materializer): (MesosClient, StateSnapshot, Flow[SchedulerCommand, StateEvent, NotUsed]) = {
    val client: MesosClient = Await.result(MesosClient(clientSettings, frameworkInfo).runWith(Sink.head), 10.seconds)
    val factory = new SchedulerFactory(client, InMemoryPodRecordRepository(), SchedulerSettings.load(), DummyMetrics)
    val (snapshot, schedulerFlow) =
      Await.result(factory.newSchedulerFlow(), 10.seconds)
    (client, snapshot, schedulerFlow)
  }

  def run(settings: MesosClientSettings)(implicit system: ActorSystem): CoreHelloWorldFramework = {
    implicit val mat: ActorMaterializer = ActorMaterializer()
    val (client, _, schedulerFlow) = init(settings, InMemoryPodRecordRepository(), buildFrameworkInfo)

    // A trick to make our stream run, even after the initial element (snapshot) is consumed. We use Source.maybe
    // which emits a materialized promise which when completed with a Some, that value will be produced downstream,
    // followed by completion. To avoid stream completion we never complete the promise but prepend the stream with
    // our snapshot together with an empty source for subsequent SpecUpdates (which we're not going to send)
    //
    // Note: this is hardly realistic since an orchestrator will need to react to StateEvents by sending SpecUpdates
    // to the scheduler. We're making our lives easier by ignoring this part for now - all we care about is to start
    // a "hello-world" task once.
    val completed: Future[Done] = Source.maybe
      .prepend(Source.single(generateLaunchCommand))
      // Here our initial snapshot is going to the scheduler flow
      .via(schedulerFlow)
      .map {
        // Main state event handler. We log happy events and exit if something goes wrong
        case PodStatusUpdatedEvent(id, Some(PodStatus(_, taskStatuses))) =>
          import TaskState._
          def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)

          // We're only interested in the happy task statuses for our pod
          val failedTasks = taskStatuses.filterNot { case (_, status) => activeTask(status) }
          assert(failedTasks.isEmpty, s"Found failed tasks: $failedTasks, can't handle them now so will terminate")

          logger.info(s"Task status updates for podId: $id: ${taskStatuses}")

        case e =>
          logger.warn(s"Unhandled event: $e") // we ignore everything else for now
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()

    CoreHelloWorldFramework(client.frameworkId, client.killSwitch, completed)
  }
}
