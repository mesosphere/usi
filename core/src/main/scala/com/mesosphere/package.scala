package com

import com.typesafe.scalalogging.CanLog
import com.typesafe.scalalogging.LoggerTakingImplicit
import org.slf4j.MDC
import scala.language.implicitConversions

package object mesosphere {

  /**
   * Abstractions in this class were designed with below interests:
   * 1. Use the slf4j abstraction without any backend (such as logback etc.,). Consumers of this library should be
   *    free to chose whatever backend they wish to use. `MDC` is a well-known concept in java world and many logging
   *    backends will be able to do a structured logging that includes MDC.
   * 2. Make it easy to compose loggers with context. E.g.: [[Traversable]] of tuples of type ([[String]], [[Any]]).
   * 3. User should be able to :
   *     - create a logger instance `A` with few context parameters associated with it.
   *     - Use the above logger for making log statements with the context built.
   *     - Pass the instance `A` to another method or class which has no information about context built so far
   *       but can chose to built context further on top of the current context OR start fresh.
   *
   * Usability primer:
   *
   * The types declared here such as [[KvArgs]] and [[LoggerAndContext]] can be used through out the project.
   * Following snippet shows an example of how the [[LoggerTakingImplicit]] can be used with the
   * abstractions defined here :
   *
   * {{{
   *    // import all the implicit defined here in to scope.
   *    import com.mesosphere._
   *
   *    // Create an instance of LoggerTakingImplicit.
   *    // This needs an evidence of CanLog[KvArgs] to compile which was brought in to scope by above import.
   *    val log = Logger.takingImplicit[KvArgs](getClass)
   *
   *    // Make a log statement with no context.
   *    // This needs an implicit context but the above import bought an empty context in to scope.
   *    log.info("hello world!")
   *
   *    // Make a log statement with some context.
   *    log.info("hello world!")(("key1", "value1"))
   *
   *    // Make a log statement with more context.
   *    log.info("hello world!")(("key1", "value1").and("key2", "value2").and("key3", "value3"))
   *
   *    // Now compose the logger with static meta data.
   *    // The withContext method here is defined on a value class of LoggerTakingImplicit.
   *    val composedLogger : LoggerAndContext = log.withContext("constantKey", "constantValue")
   *
   *    // Note that the type of composedLogger is just a simple tuple containing the logger as well as context.
   *    // The state of logger object itself is not mutated in any manner.
   *    // The logger is not knowledgeable about the context in any manner. The tuple is the ONLY thing that
   *    // is used correlate between a logger and the context associated with it.
   *
   *    // We can also compose with multiple context params
   *    val anotherLogger : LoggerAndContext = log
   *      .withContext("key1", "value1") // `withContext` is from LoggerTakingImplicitOps value class.
   *      .withContext( // `withContext` here is from LoggerAndContextOps
   *        ("key2", "value2").and("key3", "value3")) // `and` is defined in implicit value class of KvArgs
   *      )
   *      .withContext(composedLogger.context.and(""))   // Add another logger context if needed.
   *
   *    // Make some log statements using `anotherLogger`
   *    // Note that `anotherLogger` is a tuple but we are able to call logger methods directly on this tuple
   *    // because it is implicitly converted to a logger on demand.
   *    // Note that the context here is passed explicitly to the info method of `LoggerTakingImplicit`
   *    anotherLogger.info("my custom format {}", "is successful!")(anotherLogger.context)
   *
   *    // The above statement will log through whatever backend slf4j was tied to statically.
   *    // All the key value parameters in context are populated in MDC just before the log statement is made and are
   *    // deleted immediately after. This happens every time the logger object is used to generate a log statement.
   * }}}
   */

  type KvArgs = Traversable[(String, Any)]
  type LoggerAndContext = (LoggerTakingImplicit[KvArgs], KvArgs)

  /**
   * By default, we use an empty context for all loggers. This is needed where we have a class/object
   * level [[LoggerTakingImplicit]] that needs an implicit [[KvArgs]] but it may not *always* be needed
   * to have an implicit [[KvArgs]] in scope. In those cases, we use this empty implicit args.
   */
  implicit val emptyKvArgs: KvArgs = Traversable()

  implicit def tupleToTraversable(kv: (String, Any)): KvArgs = Traversable(kv)

  implicit final class KvArgsOps(val self: KvArgs) extends AnyVal {
    /**
     * Helper methods to enable fluent composition of context parameter(s).
     */
    def and(kv: KvArgs): KvArgs = self ++ kv

    def and(k: String, v: String): KvArgs = self and ((k, v))
  }

  /**
   * Evidence needed for [[LoggerTakingImplicit]] of type [[KvArgs]]
   */
  implicit case object CanLogKvArgs extends CanLog[KvArgs] {
    override def logMessage(originalMsg: String, a: KvArgs): String = {
      for (elem <- a) {
        val (key, value) = elem
        MDC.put(key, value.toString)
      }
      originalMsg
    }

    override def afterLog(a: KvArgs): Unit = a.foreach { case (key, _) => MDC.remove(key) }
  }

  /**
   * Enable calling logger methods directly on the tuple object.
   *
   * Note that we SHOULD NEVER override or re-implement the `info` or `error` methods as it may interfere
   * with the caller context.
   */
  implicit def tupleToLogger(
    tuple: LoggerAndContext
  ): LoggerTakingImplicit[KvArgs] = {
    val (logger, _) = tuple
    logger
  }

  implicit def loggerToTuple(
    logger: LoggerTakingImplicit[KvArgs]
  ): LoggerAndContext = {
    (logger, emptyKvArgs)
  }

  /**
   * Used to enrich an [[LoggerTakingImplicit]] to [[LoggerAndContext]] using given context
   */
  implicit final class LoggerTakingImplicitOps(val logger: LoggerTakingImplicit[KvArgs]) extends AnyVal {
    def withContext(key: String, value: Any): LoggerAndContext = (logger, (key, value))

    def withContext(tag: KvArgs): LoggerAndContext = (logger, tag)
  }

  /**
   * Used to add context parameters to an instance of [[LoggerAndContext]]
   */
  implicit final class LoggerAndContextOps(
    val self: LoggerAndContext
  ) extends AnyVal {

    def context: KvArgs = {
      val (_, context) = self
      context
    }

    def withContext(key: String, value: Any): LoggerAndContext = {
      val (logger, context) = self
      (logger, context ++ tupleToTraversable((key, value)))
    }

    def withContext(newContext: KvArgs): LoggerAndContext = {
      val (logger, context) = self
      (logger, context ++ newContext)
    }
  }
}
