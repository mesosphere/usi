package com.mesosphere.usi.examples

import akka.Done
import java.util.UUID
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitch, Materializer}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.core.models.{
  Goal,
  PodId,
  PodSpec,
  PodStatus,
  PodStatusUpdated,
  RunSpec,
  SpecUpdated,
  SpecsSnapshot,
  StateEvent,
  StateSnapshot
}
import com.mesosphere.usi.repository.{InMemoryPodRecordRepository, PodRecordRepository}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.{FrameworkID, FrameworkInfo, TaskState, TaskStatus}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.sys.SystemProperties
import scala.util.Failure
import scala.util.Success

/**
  * Run the hello-world example framework that:
  *  - relies on our `core` module to handle Mesos connection, event handling, offer matching etc. as
  *    opposed to the `simple-hello-world` framework which uses only the Mesos client and has to implement
  *    all of the above mentioned pieces itself
  *  - starts one `echo "Hello, world" && sleep N` task
  *  - exits when the task fails (or finishes)
  *
  *  Good to test against local Mesos.
  *
  */
case class CoreHelloWorldFramework(frameworkId: FrameworkID, killSwitch: KillSwitch, result: Future[Done])

object CoreHelloWorldFramework extends StrictLogging {

  /**
    * The type of [[SchedulerFlow]] might look scary at the first glance (and sometimes even at second) but it is
    * at the core (!) of any framework. Basically it is a Flow that [[SpecUpdated]] events as input and produces
    * [[StateEvent]] as an output.
    *
    * +--------------------+         +-----------------------+        +-----------------------+
    * |                    |         |                       |        |                       |
    * |     SpecUpdated    |         |                       |        |      StateEvent       |
    * |                    +--------->  Core SchedulerFlow   +-------->                       |
    * |   new and updated  |         |                       |        | replicated Pod, Agent |
    * |       PodSpecs     |         |                       |        | and Reservations state|
    * +--------------------+         +-----------------------+        +-----------------------+
    *
    *
    * In a nutshell: frameworks sends [[PodSpec]] updates down the flow and receives [[StateEvent]]s when things
    * change. So why is the input parameter of that Flow not simply a [[SpecUpdated]] but a tuple of
    * [[SpecsSnapshot]] and Source[[SpecUpdated]]? This is because the first message sent to the scheduler
    * by the framework should be a snapshot of all PodSpecs known to the framework - a [[SpecsSnapshot]]. Internally
    * this will trigger [task reconciliation](http://mesos.apache.org/documentation/latest/reconciliation/) by the
    * scheduler.
    *
    * The same logic applies for the output parameter of the scheduler Flow - it's a tuple of [[StateSnapshot]] and a
    * Source of [[StateEvent]], which means that the first state event received by the framework will be a
    * snapshot of all reconciled tasks states, followed by a individual updates.
    *
    * For more about the scheduler flow see [[Scheduler]] class documentation.
    *
    */
  type SchedulerFlow =
    Flow[(SpecsSnapshot, Source[SpecUpdated, Any]), (StateSnapshot, Source[StateEvent, Any]), NotUsed]

  def main(args: Array[String]): Unit = {
    implicit val actorSystem = ActorSystem()
    implicit val ec = actorSystem.dispatcher
    val conf = ConfigFactory.load().getConfig("mesos-client")
    try {
      run(conf).result.onComplete {
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

  def generateSpecSnapshot: SpecsSnapshot = {
    // Lets construct the initial specs snapshot which will contain our hello-world PodSpec. For that we generate
    // - a unique PodId
    // - a RunSpec with minimal resource requirements and hello-world shell command
    // - a snapshot containing our PodSpec
    val podId = PodId(s"hello-world.${UUID.randomUUID()}")
    val runSpec = RunSpec(
      resourceRequirements = List(ScalarRequirement.cpus(0.1), ScalarRequirement.memory(32)),
      shellCommand = """echo "Hello, world" && sleep 123456789"""
    )
    val podSpec = PodSpec(
      id = podId,
      goal = Goal.Running,
      runSpec = runSpec
    )

    SpecsSnapshot(
      podSpecs = Seq(podSpec),
      reservationSpecs = Seq.empty
    )
  }

  def buildGraph(
      conf: Config,
      podRecordRepository: PodRecordRepository,
      frameworkInfo: FrameworkInfo
  )(implicit system: ActorSystem, materializer: Materializer): (MesosClient, SchedulerFlow) = {
    val clientSettings = MesosClientSettings(conf.getString("master-url"))
    val client: MesosClient = Await.result(MesosClient(clientSettings, frameworkInfo).runWith(Sink.head), 10.seconds)
    (client, Scheduler.fromClient(client, podRecordRepository))
  }

  def run(conf: Config)(implicit system: ActorSystem): CoreHelloWorldFramework = {
    implicit val mat = ActorMaterializer()
    val (client, schedulerFlow) = buildGraph(conf, InMemoryPodRecordRepository(), buildFrameworkInfo)
    val specsSnapshot = generateSpecSnapshot

    // A trick to make our stream run, even after the initial element (snapshot) is consumed. We use Source.maybe
    // which emits a materialized promise which when completed with a Some, that value will be produced downstream,
    // followed by completion. To avoid stream completion we never complete the promise but prepend the stream with
    // our snapshot together with an empty source for subsequent SpecUpdates (which we're not going to send)
    //
    // Note: this is hardly realistic since an orchestrator will need to react to StateEvents by sending SpecUpdates
    // to the scheduler. We're making our lives easier by ignoring this part for now - all we care about is to start
    // a "hello-world" task once.
    val result: Future[Done] = Source.maybe
      .prepend(Source.single(specsSnapshot))
      .map(snapshot => (snapshot, Source.empty))
      // Here our initial snapshot is going to the scheduler flow
      .via(schedulerFlow)
      // We flatten the output of the scheduler flow which is a tuple of an initial snapshot and a source of all
      // later updates, into one stream where the first element is the snapshot and all later elements are single
      // state events. This makes the event handling a simple match-case
      .flatMapConcat {
        case (snapshot, updates) =>
          updates.prepend(Source.single(snapshot))
      }
      .map {
        // Main state event handler. We log happy events and exit if something goes wrong
        case PodStatusUpdated(id, Some(PodStatus(_, taskStatuses))) =>
          import TaskState._
          def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)

          // We're only interested in the happy task statuses for our pod
          val failedTasks = taskStatuses.filterNot { case (id, status) => activeTask(status) }
          assert(failedTasks.isEmpty, s"Found failed tasks: $failedTasks, can't handle them now so will terminate")

          logger.info(s"Task status updates for podId: $id: ${taskStatuses}")

        case e =>
          logger.warn(s"Unhandled event: $e") // we ignore everything else for now
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()

    CoreHelloWorldFramework(client.frameworkId, client.killSwitch, result)
  }
}
