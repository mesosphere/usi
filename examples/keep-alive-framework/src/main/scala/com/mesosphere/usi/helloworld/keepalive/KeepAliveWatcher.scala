package com.mesosphere.usi.helloworld.keepalive

import akka.{NotUsed}
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.models.{Goal, PodId, PodSpec, PodSpecUpdated, PodStatus, PodStatusUpdated, SpecUpdated, StateEvent, StateSnapshot}
import com.mesosphere.usi.helloworld.runspecs.{RunSpecService, RunSpecInstanceId}
import com.typesafe.scalalogging.LazyLogging
import org.apache.mesos.v1.Protos.{TaskState, TaskStatus}

import scala.concurrent.{ExecutionContext, Future}

class KeepAliveWatcher(appsService: RunSpecService)(implicit val ec: ExecutionContext) extends LazyLogging {

  // KeepAliveWatcher looks for a terminal task and then restarts the whole pod.
  val flow: Flow[StateEvent, SpecUpdated, NotUsed] = Flow[StateEvent]
    .map {
    // Main state event handler. We log happy events and restart the pod if something goes wrong
    case s: StateSnapshot =>
      logger.info(s"Initial state snapshot: $s")
      DoNothing

    case PodStatusUpdated(id, Some(PodStatus(_, taskStatuses))) =>
      import TaskState._
      def activeTask(status: TaskStatus) = Seq(TASK_STAGING, TASK_STARTING, TASK_RUNNING).contains(status.getState)
      // We're only interested in the bad task statuses for our pod
      val failedTasks = taskStatuses.filterNot { case (id, status) => activeTask(status) }
      if (failedTasks.nonEmpty) {
        logger.info(s"Restarting Pod $id")
        SpawnNewIncarnation(id)
      } else {
        DoNothing
      }

    case e =>
      logger.warn(s"Unhandled event: $e") // we ignore everything else for now
      DoNothing
  }
    .mapAsync(1) {

      case DoNothing =>
        Future.successful(Nil)

      case SpawnNewIncarnation(podId) =>
        val instanceId = RunSpecInstanceId.fromPodId(podId)
        val newPodId = instanceId.nextIncarnation.toPodId

        appsService.findRunSpec(instanceId.runSpecId).map {
          // AppInfo found
          case Some(appInfo) =>
              List(
                PodSpecUpdated(podId, None), // Remove the currentPod
                PodSpecUpdated(newPodId, Some(PodSpec(newPodId, Goal.Running, appInfo.runSpec))) // Launch the new pod
              )

          // AppInfo was deleted
          case None =>
          Nil
        }
    }
    .mapConcat(identity)



  sealed trait KeepAliveWatcherCommand
  case class SpawnNewIncarnation(podId: PodId) extends KeepAliveWatcherCommand
  case object DoNothing extends KeepAliveWatcherCommand

}
