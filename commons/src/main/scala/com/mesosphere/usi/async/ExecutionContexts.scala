package com.mesosphere.usi.async

import java.util.concurrent.{Executor, ExecutorService, Executors, ThreadFactory}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

private[async] object CallerThreadExecutionContext {
  val executor: Executor = (command: Runnable) => command.run()

  lazy val callerThreadExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executor)

  def apply(): ExecutionContext = callerThreadExecutionContext
}

object ThreadPoolContext {

  private val numberOfThreads: Int = System.getProperty("numberOfIoThreads", "100").toInt

  /**
    * This execution context is backed by a cached thread pool.
    * Use this context instead of the global execution context,
    * if you do blocking IO operations.
    */
  implicit lazy val ioContext =
    ExecutionContexts.fixedThreadPoolExecutionContext(numberOfThreads, namePrefix = "io-pool")
}

object ExecutionContexts {

  /**
    * An [[ExecutionContext]] that uses the same thread. Use it wisely.
    */
  lazy val callerThread: ExecutionContext = CallerThreadExecutionContext()

  /**
    * Returns an execution context backed by a fixed thread pool. All threads in the pool are prefixed with `namePrefix`
    * e.g. `slow-io-pool-thread-1`.
    *
    * @param numThreads number of threads in the pool
    * @param namePrefix thread name prefix
    * @return execution context
    */
  def fixedThreadPoolExecutionContext(numThreads: Int, namePrefix: String): ExecutionContext = {
    val factory: ThreadFactory = new ThreadFactoryBuilder().setNameFormat(s"$namePrefix-thread-%d").build()
    val executorService: ExecutorService = Executors.newFixedThreadPool(numThreads, factory)
    ExecutionContext.fromExecutorService(executorService)
  }
}
