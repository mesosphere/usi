package com.mesosphere.usi.core

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

// TODO : This is a duplicated in com.mesosphere.usi.async. Ideally, this needs to be in a shared module.
private[usi] object CallerThreadExecutionContext {
  val executor: Executor = (command: Runnable) => command.run()

  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
}
