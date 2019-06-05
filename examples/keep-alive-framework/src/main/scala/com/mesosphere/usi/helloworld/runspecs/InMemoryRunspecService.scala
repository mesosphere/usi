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
  RunTemplate,
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
class InMemoryServiceController(
    specSink: Sink[SchedulerCommand, NotUsed],
    stateSource: Source[StateEventOrSnapshot, NotUsed])(implicit mat: Materializer, ec: ExecutionContext)
    extends ServiceController
    with LazyLogging {

  private val specUpdateQueue = Source
    .queue[List[SchedulerCommand]](8, OverflowStrategy.dropNew)
    .mapConcat(identity)
    .to(specSink)
    .run()

  private val state: ConcurrentHashMap[ServiceSpecId, RunSpecState] =
    new ConcurrentHashMap[ServiceSpecId, RunSpecState]()

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

          val appId = ServiceSpecInstanceId.fromPodId(id).serviceSpecId

          state.computeIfPresent(appId, { (appId, state) =>
            state.copy(status = newPodStaus)
          })

        case other =>
      }

  }

  override def launchServiceFromSpec(id: ServiceSpecId, runSpec: RunTemplate): Future[LaunchResult] = {

    if (state.containsKey(id)) {
      Future.successful(LaunchResults.AlreadyExist)
    } else {

      val incarnation = 1
      val runSpecInstanceId = ServiceSpecInstanceId(id, InstanceId(UUID.randomUUID()), incarnation)

      val specUpdated = LaunchPod(runSpecInstanceId.toPodId, runSpec)

      specUpdateQueue.offer(specUpdated :: Nil).map {
        case QueueOfferResult.Enqueued =>
          val applicationState = RunSpecState(runSpecInstanceId, runSpec, Staging)
          state.putIfAbsent(id, applicationState)
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
    val results = state.elements().asScala.map(state2appInfo).toVector

    Future.successful(results)
  }

  override def findRunSpec(id: ServiceSpecId): Future[Option[RunSpecInfo]] = {
    state.get(id) match {
      case null => Future.successful(None)
      case state => Future.successful(Some(state2appInfo(state)))
    }
  }

  override def wipeRunspec(id: ServiceSpecId): Future[WipeResult] = {
    if (!state.containsKey(id)) {
      Future.successful(WipeResults.Wiped)
    } else {

      val appState = state.get(id)

      val podId = appState.runSpecInstanceId.toPodId

      val killPod = KillPod(podId)
      val expungePod = ExpungePod(podId)
      specUpdateQueue.offer(killPod :: expungePod :: Nil).map {
        case QueueOfferResult.Enqueued =>
          // TODO remove it once USI will provide the needed events.
          state.remove(id)
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
    RunSpecInfo(state.runSpecInstanceId.serviceSpecId, state.runSpec, state.status.toString)
  }
}

private[runspecs] sealed trait Status
private[runspecs] case object Staging extends Status
private[runspecs] case object Running extends Status
private[runspecs] case object Finished extends Status
private[runspecs] case class RunSpecState(
    runSpecInstanceId: ServiceSpecInstanceId,
    runSpec: RunTemplate,
    status: Status)
