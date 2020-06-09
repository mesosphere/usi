package com.mesosphere.usi.async

import java.util.concurrent.Executor

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * An [[ExecutionContext]] that uses the same thread. Use it wisely.
  */
object CallerThreadExecutionContext {
  val executor: Executor = (command: Runnable) => command.run()

  lazy val callerThreadExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executor)

  def apply(): ExecutionContext = callerThreadExecutionContext
}

object ExecutionContexts {
  lazy val callerThread: ExecutionContext = CallerThreadExecutionContext()
}
