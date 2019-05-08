package com.mesosphere.usi.helloworld.runspecs

import java.util.UUID

import com.mesosphere.usi.core.models.{PodId, RunSpec}

import scala.concurrent.Future

trait RunSpecService {
  def launchRunSpec(id: RunSpecId, runSpec: RunSpec): Future[LaunchResult]
  def listRunSpecs(): Future[Vector[RunSpecInfo]]
  def findRunSpec(id: RunSpecId): Future[Option[RunSpecInfo]]
  def wipeRunspec(id: RunSpecId): Future[WipeResult]
}

sealed trait LaunchResult

object LaunchResults {
  case object AlreadyExist extends LaunchResult
  case class Launched(id: RunSpecInstanceId) extends LaunchResult
  case object TooMuchLoad extends LaunchResult
  case class Failed(cause: Throwable) extends LaunchResult
}

sealed trait WipeResult

object WipeResults {
  case object Wiped extends WipeResult
  case object TooMuchLoad extends WipeResult
  case class Failed(cause: Throwable) extends WipeResult
}

case class RunSpecId(value: String)
case class DeploymentId(value: String)

case class RunSpecInfo(id: RunSpecId, runSpec: RunSpec, status: String)


case class RunSpecInstanceId(runSpecId: RunSpecId, instanceId: UUID, incarnation: Long) {
  def toPodId: PodId = PodId(s"${runSpecId.value}.${instanceId.toString}.$incarnation")
  def nextIncarnation: RunSpecInstanceId = {
    RunSpecInstanceId(runSpecId, instanceId, incarnation + 1)
  }


  override def toString: String = {
    s"${runSpecId.value}.${instanceId.toString}"
  }
}

object RunSpecInstanceId {
  def fromPodId(podId: PodId): RunSpecInstanceId = {
    val runSpecInstanceIdRegex = """^(.+)\.(.+)\.(\d+)$""".r
    val (appId, instanceId, currentIncarnation) = podId.value match {
      case runSpecInstanceIdRegex(id, instanceId, inc) =>
        (id, UUID.fromString(instanceId), inc.toLong)
    }
    RunSpecInstanceId(RunSpecId(appId), instanceId, currentIncarnation)
  }
}
