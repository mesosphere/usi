package com.mesosphere.usi.interface

import com.mesosphere.usi.interface.DummySchedulerInterface.DeploymentId

import scala.concurrent.Future

trait DummySchedulerInterface {
  def schedule(podSpec: PodSpec): Future[DeploymentId]
}

object DummySchedulerInterface {
  type DeploymentId = String
}

/**
  * The pod sec describes a Mesos task group that should be launched.
  */
trait PodSpec {
  def id: String
  def goal: String
  def runSpec: String
}

