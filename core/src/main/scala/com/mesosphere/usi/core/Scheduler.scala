package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.PodSpec

import scala.concurrent.Future


class Scheduler  {

  def schedule(podSpec: PodSpec): Future[String] = {
    println(s"scheduling pod ${podSpec.id}")
    Future.successful("deploymentId")
  }
}
