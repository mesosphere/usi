package com.mesosphere.usi.async

import akka.actor.ActorSystem
import akka.pattern.after

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, blocking => blockingCall}

/**
  * Function transformations to make a method timeout after a given duration.
  */
object Timeout {

  import DurationOps._

  /**
    * Timeout a blocking call
    * @param timeout The maximum duration the method may execute in
    * @param name Name of the operation
    * @param f The blocking call
    * @param system The Akka system
    * @param ctx The execution context to execute 'f' in
    * @tparam T The result type of 'f'
    * @return The eventual result of calling 'f' or TimeoutException if it didn't complete in time.
    */
  def blocking[T](timeout: FiniteDuration, name: Option[String] = None)(
      f: => T
  )(implicit system: ActorSystem, ctx: ExecutionContext): Future[T] =
    apply(timeout, name)(Future(blockingCall(f))(ctx))(system, ctx)

  /**
    * Timeout a non-blocking call.
    * @param timeout The maximum duration the method may execute in
    * @param name Name of the operation
    * @param f The blocking call
    * @param system The Akka system
    * @param ctx The execution context to execute 'f' in
    * @tparam T The result type of 'f'
    * @return The eventual result of calling 'f' or TimeoutException if it didn't complete
    */
  def apply[T](timeout: Duration, name: Option[String] = None)(
      f: => Future[T]
  )(implicit system: ActorSystem, ctx: ExecutionContext): Future[T] = {
    require(timeout != Duration.Zero)

    timeout match {
      case duration: FiniteDuration =>
        def t: Future[T] =
          after(duration, system.scheduler)(
            Future.failed(new TimeoutException(s"${name.getOrElse("None")} timed out after ${timeout.toHumanReadable}"))
          )
        Future.firstCompletedOf(Seq(f, t))
      case _ => f
    }
  }
}
