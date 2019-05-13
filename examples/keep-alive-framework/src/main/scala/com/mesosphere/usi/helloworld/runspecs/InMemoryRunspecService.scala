package com.mesosphere.usi.helloworld.runspecs
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.mesosphere.usi.core.models.{
  ExpungePod,
  KillPod,
  LaunchPod,
  PodStatusUpdatedEvent,
  RunSpec,
  SchedulerCommand,
  StateEventOrSnapshot
}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import org.apache.mesos.v1.Protos.TaskState._

/**
  * Simple in-memory implementation of the RunSpecService.
  *
  * Not very thread-safe and not ready for production.
  *
  * @param specSink
  * @param mat
  * @param ec
  */
class InMemoryDemoRunSpecService(
    specSink: Sink[SchedulerCommand, NotUsed],
    stateSource: Source[StateEventOrSnapshot, NotUsed])(implicit mat: Materializer, ec: ExecutionContext)
    extends RunSpecService
    with LazyLogging {

  private val specUpdateQueue = Source
    .queue[List[SchedulerCommand]](8, OverflowStrategy.dropNew)
    .mapConcat(identity)
    .to(specSink)
    .run()

  private val database: ConcurrentHashMap[RunSpecId, RunSpecState] =
    new ConcurrentHashMap[RunSpecId, RunSpecState]()

  stateSource.runForeach {
    case event: StateEventOrSnapshot =>
      logger.info(event.toString)
      event match {

        case PodStatusUpdatedEvent(id, Some(newStatus)) =>
          // TODO fix this one pod status will be there
          val newPodStaus = newStatus.taskStatuses.values.headOption
            .map(_.getState)
            .map {
              case TASK_STAGING | TASK_STARTING =>
                Staging

              case TASK_RUNNING =>
                Running

              case _ =>
                Finished

            }
            .getOrElse(Finished) // no tasks found

          val appId = RunSpecInstanceId.fromPodId(id).runSpecId

          database.computeIfPresent(appId, { (appId, state) =>
            state.copy(status = newPodStaus)
          })

        case other =>
      }

  }

  override def launchRunSpec(id: RunSpecId, runSpec: RunSpec): Future[LaunchResult] = {

    if (database.containsKey(id)) {
      Future.successful(LaunchResults.AlreadyExist)
    } else {

      val incarnation = 1
      val runSpecInstanceId = RunSpecInstanceId(id, UUID.randomUUID(), incarnation)

      val specUpdated = LaunchPod(runSpecInstanceId.toPodId, runSpec)

      specUpdateQueue.offer(specUpdated :: Nil).map {
        case QueueOfferResult.Enqueued =>
          val applicationState = RunSpecState(runSpecInstanceId, runSpec, Staging)
          database.putIfAbsent(id, applicationState)
          LaunchResults.Launched(runSpecInstanceId)

        case QueueOfferResult.Dropped =>
          LaunchResults.TooMuchLoad

        case QueueOfferResult.Failure(ex) =>
          LaunchResults.Failed(ex)

        case QueueOfferResult.QueueClosed =>
          LaunchResults.Failed(new Exception("queue was closed"))
      }
    }

  }

  override def listRunSpecs(): Future[Vector[RunSpecInfo]] = {
    val results = database.elements().asScala.map(state2appInfo).toVector

    Future.successful(results)
  }

  override def findRunSpec(id: RunSpecId): Future[Option[RunSpecInfo]] = {
    database.get(id) match {
      case null => Future.successful(None)
      case state => Future.successful(Some(state2appInfo(state)))
    }
  }

  override def wipeRunspec(id: RunSpecId): Future[WipeResult] = {
    if (!database.containsKey(id)) {
      Future.successful(WipeResults.Wiped)
    } else {

      val appState = database.get(id)

      val podId = appState.runSpecInstanceId.toPodId

      val killPod = KillPod(podId)
      val expungePod = ExpungePod(podId)
      specUpdateQueue.offer(killPod :: expungePod :: Nil).map {
        case QueueOfferResult.Enqueued =>
          // TODO remove it once USI will provide the needed events.
          database.remove(id)
          WipeResults.Wiped

        case QueueOfferResult.Dropped =>
          WipeResults.TooMuchLoad

        case QueueOfferResult.Failure(ex) =>
          WipeResults.Failed(ex)

        case QueueOfferResult.QueueClosed =>
          WipeResults.Failed(new Exception("queue was closed"))
      }
    }
  }

  private def state2appInfo(state: RunSpecState): RunSpecInfo = {
    RunSpecInfo(state.runSpecInstanceId.runSpecId, state.runSpec, state.status.toString)
  }
}

private[runspecs] sealed trait Status
private[runspecs] case object Staging extends Status
private[runspecs] case object Running extends Status
private[runspecs] case object Finished extends Status
private[runspecs] case class RunSpecState(runSpecInstanceId: RunSpecInstanceId, runSpec: RunSpec, status: Status)
