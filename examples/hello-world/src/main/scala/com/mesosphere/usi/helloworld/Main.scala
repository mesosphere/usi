package com.mesosphere.usi.helloworld

import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models.{PodId, PodSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * A demonstration of USI features.
  */
object Main extends App {

  val scheduler =  new Scheduler

  println("Starting orchestrator")

  val podSpec = PodSpec(PodId("myPod"))

  val res = Await.result(scheduler.schedule(podSpec), Duration.Inf)

  println(s"pod scheduled with depoloymentId: $res")

}
