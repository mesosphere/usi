package com.mesosphere.utils

import com.typesafe.scalalogging.{Logger, StrictLogging}

import scala.sys.process.ProcessLogger

/**
  * We override [[ProcessLogger]] to forward Mesos Agent/Master and Framework process output (stdout and stderr)
  * to the logger. Useful to have all processes log corresponding to a test (master, agent and framework) in one
  * place but still easily distinguishable by the prefix.
  *
  * @param processName wrapped process name
  */
case class ProcessOutputToLogStream(processName: String) extends ProcessLogger with StrictLogging {
  override val logger = Logger(processName)
  override def out(msg: => String): Unit = logger.debug(msg)
  override def err(msg: => String): Unit = logger.warn(msg)
  override def buffer[T](f: => T): T = f
}
