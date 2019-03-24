package com.mesosphere.usi.core

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

private[usi] object CallerThreadExecutionContext {
  val executor: Executor = (command: Runnable) => command.run()

  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
}
