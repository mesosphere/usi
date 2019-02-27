package com.mesosphere.usi.helloworld

import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.SinkQueueWithCancel
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.matching.ScalarResource
import com.mesosphere.usi.core.models._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.TaskState.{TASK_RUNNING, TASK_STAGING, TASK_STARTING}
import org.apache.mesos.v1.Protos.{FrameworkInfo, TaskState, TaskStatus}

import scala.concurrent.{Await, Future}
import scala.sys.SystemProperties
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


class KeepAliveFramework(conf: Config) extends StrictLogging {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val settings = MesosClientSettings(conf.getString("master-url"))
  val frameworkInfo = FrameworkInfo
    .newBuilder()
    .setUser(
      new SystemProperties()
        .get("user.name")
        .getOrElse(throw new IllegalArgumentException("A local user is needed to launch Mesos tasks")))
    .setName("CoreHelloWorldExample")
    .setFailoverTimeout(0d)
    .build()

  val client: MesosClient = Await.result(MesosClient(settings, frameworkInfo).runWith(Sink.head), 10.seconds)

  val schedulerFlow
  : Flow[(SpecsSnapshot, Source[SpecUpdated, Any]), (StateSnapshot, Source[StateEvent, Any]), NotUsed] =
    Scheduler.fromClient(client)


  val podId = PodId(s"hello-world.${UUID.randomUUID()}.1")
  val runSpec = RunSpec(
    resourceRequirements =
      List(
        ScalarResource(ResourceType.CPUS, 0.1),
        ScalarResource(ResourceType.MEM, 32)),
    shellCommand = """echo "Hello, world" && sleep 3600"""
  )
  val podSpec = PodSpec(
    id = podId,
    goal = Goal.Running,
    runSpec = runSpec
  )

  val specsSnapshot = SpecsSnapshot(
    podSpecs = Seq(podSpec),
    reservationSpecs = Seq.empty
  )

  def createNewIncarnationId(podId: PodId): PodId = {
    val idAndIncarnation = """^(.+\..*)\.(\d+)$""".r
    val (currentIncarnation, podIdWithoutIncarnation) = podId.value match {
      case idAndIncarnation(id, inc) => id -> inc.toLong
    }
    PodId(s"$podIdWithoutIncarnation.${currentIncarnation+1}")
  }

  // This flow consumes state events, checks if anything failed and then issues spec commands
  val keepAliveWatcher: Flow[StateEvent, SpecUpdated, NotUsed] = Flow[StateEvent]
    .mapConcat {
      // Main state event handler. We log happy events and restart the pod if something goes wrong
      case s: StateSnapshot =>
        logger.info(s"Initial state snapshot: $s")
        Nil

      case PodStatusUpdated(id, Some(PodStatus(_, taskStatuses))) =>
        import TaskState._
        def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)

        // We're only interested in the bad task statuses for our pod
        val failedTasks = taskStatuses.filterNot { case (id, status) => activeTask(status) }

        logger.info(s"Task status updates for podId: $id: ${taskStatuses}")

        if (failedTasks.nonEmpty) {
          val newIncarnation = createNewIncarnationId(podId)
          PodSpecUpdated(id, Some(PodSpec(newIncarnation, Goal.Running, runSpec))) :: Nil
        } else {
          Nil
        }

      case e =>
        logger.error(s"Unhandled event: $e") // we ignore everything else for now
        Nil
    }

  // to communicate with keep alive flow, we use a queue since the scheduler interface can't be nicely plugged-in here
  val (keepAliveStateQueue, keepAliveStateSource) =
    Source.queue[StateEvent](100, OverflowStrategy.dropNew).preMaterialize()

  val (keepAliveSpecQueue, keepAliveSpecSink) =
    Sink.queue[SpecUpdated]().preMaterialize()


  keepAliveStateSource.via(keepAliveWatcher).to(keepAliveSpecSink).run()


  val running = Source.single(specsSnapshot)
    // A trick to make our stream run, we use a workaround to transform the sink of our keep alive mechanism into
    // a source. If we change the scheduler interface this will be much more elegant
    .map(snapshot => (snapshot, Source.unfoldResourceAsync[SpecUpdated, SinkQueueWithCancel[SpecUpdated]](
      () => Future.successful(keepAliveSpecQueue),
      q => q.pull(),
      q => { q.cancel(); Future.successful(Done)}
    )))
    // Here our initial snapshot is going to the scheduler flow
    .via(schedulerFlow)
    // We flatten the output of the scheduler flow which is a tuple of an initial snapshot and a source of all
    // later updates, into one stream where the first element is the snapshot and all later elements are single
    // state events. This makes the event handling a simple match-case
    .flatMapConcat {
    case (snapshot, updates) =>
      updates.prepend(Source.single(snapshot))
  }
    .mapAsync(1)(keepAliveStateQueue.offer)
    .map {
      case QueueOfferResult.Enqueued =>
        // everything is good, do nothing.

      case _ =>
        logger.error(s"Something wrong happened to the keepalive mechanism")
        throw new RuntimeException("keepalive is dead!")

    }
    .toMat(Sink.ignore)(Keep.right)
    .run()

  running.onComplete {
    case Success(res) =>
      logger.info(s"Stream completed: $res");
      system.terminate()
    case Failure(e) =>
      logger.error(s"Error in stream: $e");
      system.terminate()
  }


  // We let the framework run "forever" (or at least until our task completes/fails)
  Await.result(running, Duration.Inf)

}


object KeepAliveFramework {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("mesos-client")
    KeepAliveFramework(conf)
  }

  def apply(conf: Config): KeepAliveFramework = new KeepAliveFramework(conf)
}
