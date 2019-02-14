package com

import com.typesafe.scalalogging.CanLog
import com.typesafe.scalalogging.Logger
import com.typesafe.scalalogging.LoggerTakingImplicit
import org.slf4j.MDC

package object mesosphere {

  /**
   * Abstractions in this class were designed with below interests:
   * 1. Use the slf4j abstraction without any backend (such as logback etc.,). Consumers of this library should be
   *    free to chose whatever backend they wish to use. `MDC` is a well-known concept in java world and many logging
   *    backends will be able to do a structured logging that includes MDC.
   * 2. Make it easy to compose context(s) E.g.: [[Traversable]] of tuples of type ([[String]], [[Any]]) for loggers.
   * 3. User should be able to :
   *     - Mix in one of the [[ImplicitStrictLogging]] or [[ImplicitLazyLogging]] traits
   *     - Use the logger instance `logger` to print log statements with some context parameters.
   *     - Pass the context to another method or class which has no information about context built so far
   *       and thus can chose to built context further on top of the current context OR start fresh.
   *
   * The types declared here such as [[KvArgs]] and its value classes can be used through out the project.
   * Following snippet shows an example of how the [[LoggerTakingImplicit]] can be used with the
   * abstractions defined here :
   *
   * {{{
   *    // import all the implicit defined here in to scope.
   *    import com.mesosphere._
   *
   *    // Make a log statement with no context.
   *    // This needs an implicit context but the above import bought an empty context in to scope.
   *    logger.info("hello world!")
   *
   *    // Make a log statement with some context.
   *    logger.info("hello world!")(kv("key1", "value1"))
   *
   *    // Make a log statement with more context.
   *    val moreContext = kv("key1", "value1").and("key2", "value2").and("key3", "value3")
   *    log.info("hello world!")(moreContext.and("key3", "key4"))
   *
   *    // The above statement will log through whatever backend slf4j was tied to statically.
   *    // All the key value parameters in context are populated in MDC just before the log statement is made and are
   *    // deleted immediately after. This happens every time the `logger` object is used to generate a log statement.
   * }}}
   */

  type KvArgs = Traversable[(String, Any)]

  implicit case object CanLogKvArgsEv extends CanLog[KvArgs] {
    override def logMessage(originalMsg: String, a: KvArgs): String = {
      for (elem <- a) {
        val (key, value) = elem
        MDC.put(key, value.toString)
      }
      originalMsg
    }

    override def afterLog(a: KvArgs): Unit = a.foreach { case (key, _) => MDC.remove(key) }
  }

  implicit val emptyKvArgs: KvArgs = Traversable()

  def kv(kv: (String, Any)): KvArgs = Traversable(kv)

  def kv(key: String, value: Any): KvArgs = kv((key, value))

  implicit class KvArgsOps(val self: KvArgs) extends AnyVal {
    /**
     * Helper methods to enable fluent composition of context parameter(s).
     */
    def and(kv: KvArgs): KvArgs = self ++ kv

    def and(k: String, v: String): KvArgs = self and kv(k, v)
  }

}

package mesosphere {
  /**
   * Defines `logger` as a value initialized with an underlying `org.slf4j.Logger`
   * named according to the class into which this trait is mixed.
   *
   * Also refer [[com.typesafe.scalalogging.StrictLogging]]
   */
  trait ImplicitStrictLogging {
    protected val logger: LoggerTakingImplicit[KvArgs] =
      Logger.takingImplicit[KvArgs](getClass)
  }

  /**
   * Defines `logger` as a value initialized with an underlying `org.slf4j.Logger`
   * named according to the class into which this trait is mixed.
   *
   * Also refer [[com.typesafe.scalalogging.LazyLogging]]
   */
  trait ImplicitLazyLogging {
    @transient
    protected val logger: LoggerTakingImplicit[KvArgs] =
      Logger.takingImplicit[KvArgs](getClass)
  }
}
