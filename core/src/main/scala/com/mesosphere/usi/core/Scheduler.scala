package com.mesosphere.usi.core

import scala.concurrent.Future
import com.mesosphere.usi.core.models.{DeploymentId, PodSpec}

class Scheduler {

  def schedule(podSpec: PodSpec): Future[DeploymentId] = {
    println(s"scheduling pod ${podSpec.id}")
    Future.successful(DeploymentId("deploymentId"))
  }
}
