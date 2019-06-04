package com.mesosphere.usi.helloworld.runspecs

import java.util.UUID

import com.mesosphere.usi.core.models.{PodId, RunSpec}

import scala.concurrent.Future

trait ServiceController {
  def launchServiceFromSpec(id: ServiceSpecId, runSpec: RunSpec): Future[LaunchResult]
  def listRunSpecs(): Future[Vector[RunSpecInfo]]
  def findRunSpec(id: ServiceSpecId): Future[Option[RunSpecInfo]]
  def wipeRunspec(id: ServiceSpecId): Future[WipeResult]
}

sealed trait LaunchResult

object LaunchResults {
  case object AlreadyExist extends LaunchResult
  case class Launched(id: ServiceSpecInstanceId) extends LaunchResult
  case object TooMuchLoad extends LaunchResult
  case class Failed(cause: Throwable) extends LaunchResult
}

sealed trait WipeResult

object WipeResults {
  case object Wiped extends WipeResult
  case object TooMuchLoad extends WipeResult
  case class Failed(cause: Throwable) extends WipeResult
}

/**
  * User-defined name of the service
  * @param value
  */
case class ServiceSpecId(value: String)

/**
  * Id of the specific instance of the service
  * @param value
  */
case class InstanceId(value: UUID)
case class DeploymentId(value: String)

case class RunSpecInfo(id: ServiceSpecId, runSpec: RunSpec, status: String)

/**
  * ServiceSpecInstanceId identifies the pod that is launched for a specific instance of the service spec
  * @param serviceSpecId id of the service spec
  * @param instanceId id of the instance
  * @param incarnation incarnation of the instance
  */
case class ServiceSpecInstanceId(serviceSpecId: ServiceSpecId, instanceId: InstanceId, incarnation: Long) {
  def toPodId: PodId = PodId(s"${serviceSpecId.value}.${instanceId.value.toString}.$incarnation")
  def nextIncarnation: ServiceSpecInstanceId = {
    ServiceSpecInstanceId(serviceSpecId, instanceId, incarnation + 1)
  }

  override def toString: String = {
    s"${serviceSpecId.value}.${instanceId.value.toString}"
  }
}

object ServiceSpecInstanceId {
  def fromPodId(podId: PodId): ServiceSpecInstanceId = {
    val runSpecInstanceIdRegex = """^(.+)\.(.+)\.(\d+)$""".r
    val (appId, instanceId, currentIncarnation) = podId.value match {
      case runSpecInstanceIdRegex(id, instance, inc) =>
        (id, UUID.fromString(instance), inc.toLong)
    }
    ServiceSpecInstanceId(ServiceSpecId(appId), InstanceId(instanceId), currentIncarnation)
  }
}
