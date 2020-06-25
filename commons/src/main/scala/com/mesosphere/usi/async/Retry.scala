package com.mesosphere.usi.async

import java.time.Instant
import java.time
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise, blocking => blockingCall}
import scala.compat.java8.DurationConverters.DurationOps
import scala.util.{Failure, Random, Success}
import scala.util.control.NonFatal

/**
  * The retry configuration object.
  *
  * @param maxAttempts The maximum number of attempts before failing
  * @param minDelay The minimum delay between invocations
  * @param maxDelay The maximum delay between invocations
  * @param maxDuration The maximum amount of time to allow the operation to complete
  */
class RetrySettings private (
    val maxAttempts: Int,
    val minDelay: FiniteDuration,
    val maxDelay: FiniteDuration,
    val maxDuration: Duration
) {

  private def copy(
      maxAttempts: Int = this.maxAttempts,
      minDelay: FiniteDuration = this.minDelay,
      maxDelay: FiniteDuration = this.maxDelay,
      maxDuration: Duration = this.maxDuration
  ) =
    new RetrySettings(maxAttempts, minDelay, maxDelay, maxDuration)

  def withMaxAttempts(attempts: Int): RetrySettings = copy(maxAttempts = attempts)

  def withMinDelay(delay: FiniteDuration): RetrySettings = copy(minDelay = delay)

  def withMaxDelay(delay: FiniteDuration): RetrySettings = copy(maxDelay = delay)

  def withMaxDuration(duration: Duration): RetrySettings = copy(maxDuration = duration)
}

object RetrySettings {
  def fromConfig(conf: Config): RetrySettings = {
    val maxAttempts = conf.getInt("max-attempts")

    val minDelay: FiniteDuration = DurationOps(conf.getDuration("min-delay")).toScala
    val maxDelay: FiniteDuration = DurationOps(conf.getDuration("max-delay")).toScala
    val maxDuration: Duration = DurationOps(conf.getDuration("max-duration")).toScala

    new RetrySettings(maxAttempts, minDelay, maxDelay, maxDuration)
  }

  def load(): RetrySettings = {
    val conf = ConfigFactory.load();
    RetrySettings.fromConfig(conf.getConfig("usi.retry-defaults"))
  }

  def load(loader: ClassLoader): RetrySettings = {
    val conf = ConfigFactory.load(loader);
    RetrySettings.fromConfig(conf.getConfig("usi.retry-defaults"))
  }
}

/**
  * Functional transforms to retry methods using a form of Exponential Backoff with decorrelated jitter.
  *
 * See also: https://www.awsarchitectureblog.com/2015/03/backoff.html
  */
object Retry {

  import com.mesosphere.usi.async.DurationOps.DurationToHumanReadable

  val defaultSettings: RetrySettings = RetrySettings.load()

  type RetryOnFn = Throwable => Boolean
  val defaultRetry: RetryOnFn = NonFatal(_)

  private[async] def randomBetween(min: Long, max: Long): FiniteDuration = {
    require(min <= max)
    val nanos = math.min(math.abs(Random.nextLong() % (max - min + 1)) + min, max)
    FiniteDuration(nanos, TimeUnit.NANOSECONDS)
  }

  /**
    * Retry a non-blocking call
    *
    * @param maxAttempts The maximum number of attempts before failing
    * @param minDelay The minimum delay between invocations
    * @param maxDelay The maximum delay between invocations
    * @param maxDuration The maximum amount of time to allow the operation to complete
    * @param retryOn A method that returns true for Throwables which should be retried
    * @param f The method to transform
    * @param system The Akka system to execute on
    * @param ctx The execution context to run the method on
    * @tparam T The result type of 'f'
    * @return The result of 'f', TimeoutException if 'f' failed 'maxAttempts' with retry-able exceptions
    *         and the last exception that was thrown, or the last exception thrown if 'f' failed with a
    *         non-retry-able exception.
    */
  def apply[T](
      name: String,
      maxAttempts: Int = defaultSettings.maxAttempts,
      minDelay: FiniteDuration = defaultSettings.minDelay,
      maxDelay: FiniteDuration = defaultSettings.maxDelay,
      maxDuration: Duration = defaultSettings.maxDuration,
      retryOn: RetryOnFn = defaultRetry
  )(f: => Future[T])(implicit system: ActorSystem, ctx: ExecutionContext): Future[T] = {
    val promise = Promise[T]()

    require(
      maxDelay < maxDuration,
      s"maxDelay of ${maxDelay.toSeconds} seconds is larger than the maximum allowed duration: $maxDuration"
    )

    def retry(attempt: Int, lastDelay: FiniteDuration): Unit = {
      val startedAt = Instant.now()
      f.onComplete {
        case Success(result) =>
          promise.success(result)
        case Failure(e) if retryOn(e) =>
          val expired = time.Duration.between(startedAt, Instant.now()).toMillis >= maxDuration.toMillis
          if (attempt + 1 < maxAttempts && !expired) {
            val jitteredLastDelay = lastDelay.toNanos * 3
            val nextDelay = randomBetween(
              lastDelay.toNanos,
              if (jitteredLastDelay < 0 || jitteredLastDelay > maxDelay.toNanos) maxDelay.toNanos else jitteredLastDelay
            )

            require(
              nextDelay <= maxDelay,
              s"nextDelay of ${nextDelay.toNanos}ns is too big, may not exceed ${maxDelay.toNanos}ns"
            )

            system.scheduler.scheduleOnce(nextDelay)(retry(attempt + 1, nextDelay))
          } else {
            if (expired) {
              promise.failure(
                TimeoutException(
                  s"$name failed to complete in under ${maxDuration.toHumanReadable}. Last error: ${e.getMessage}",
                  e
                )
              )
            } else {
              promise.failure(
                TimeoutException(s"$name failed after $maxAttempts attempt(s). Last error: ${e.getMessage}", e)
              )
            }
          }
        case Failure(e) =>
          promise.failure(e)
      }

    }
    retry(0, minDelay)
    Timeout(maxDuration, name = Some(name))(promise.future)
  }

  /**
    * Retry a blocking call
    * @param maxAttempts The maximum number of attempts before failing
    * @param minDelay The minimum delay between invocations
    * @param maxDelay The maximum delay between invocations
    * @param maxDuration The maximum amount of time to allow the operation to complete
    * @param retryOn A method that returns true for Throwables which should be retried
    * @param f The method to transform
    * @param system The Akka system to execute on
    * @param ctx The execution context to run the method on
    * @tparam T The result type of 'f'
    * @return The result of 'f', TimeoutException if 'f' failed 'maxAttempts' with retry-able exceptions
    *         and the last exception that was thrown, or the last exception thrown if 'f' failed with a
    *         non-retry-able exception.
    */
  def blocking[T](
      name: String,
      maxAttempts: Int = defaultSettings.maxAttempts,
      minDelay: FiniteDuration = defaultSettings.minDelay,
      maxDelay: FiniteDuration = defaultSettings.maxDelay,
      maxDuration: Duration = defaultSettings.maxDuration,
      retryOn: RetryOnFn = defaultRetry
  )(f: => T)(implicit system: ActorSystem, ctx: ExecutionContext): Future[T] = {
    apply(name, maxAttempts, minDelay, maxDelay, maxDuration, retryOn)(Future(blockingCall(f)))
  }
}
