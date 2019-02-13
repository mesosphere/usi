package com.mesosphere.usi.helloworld

import com.mesosphere.usi.core.Scheduler
import com.mesosphere._
import com.typesafe.scalalogging.Logger

/**
  * A demonstration of USI features.
  */
object Main extends App {
  val composedLogger = Logger.takingImplicit[KvArgs](getClass).withCtx("hello", "world")
  val moreCtx = composedLogger.ctx.and("hostName", "localhost")
  composedLogger.info("initiating connection")(moreCtx)
  Scheduler.connect("asdasd")
}
