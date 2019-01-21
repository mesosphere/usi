package com.mesosphere.usi.core

import scala.concurrent.Future
import com.mesosphere.usi.interface.DummySchedulerInterface
import com.mesosphere.usi.interface.DummySchedulerInterface.DeploymentId
import com.mesosphere.usi.interface.PodSpec

class Scheduler extends DummySchedulerInterface {

  override def schedule(podSpec: PodSpec): Future[DeploymentId] = {
    println(s"scheduling pod ${podSpec.id} with goal ${podSpec.goal}")
    Future.successful("deploymentId")
  }
}
