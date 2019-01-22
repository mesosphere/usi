package com.mesosphere.usi.helloworld

import com.mesosphere.usi.core.{PodSpec, Scheduler}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends App {

  val scheduler =  new Scheduler

  println("Starting orchestrator")

  val podSpec = new PodSpec {
    override def id: String = "myPod"

    override def goal: String = "goal"

    override def runSpec: String = "someRunspec"
  }


  val res = Await.result(scheduler.schedule(podSpec), Duration.Inf)

  println(s"pod scheduled with depoloymentId: $res")

}
