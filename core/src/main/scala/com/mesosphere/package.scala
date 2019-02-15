package com

import com.typesafe.scalalogging.CanLog
import com.typesafe.scalalogging.Logger
import com.typesafe.scalalogging.LoggerTakingImplicit
import org.slf4j.MDC

package object mesosphere {
  implicit case object CanLogKvArgsEv extends CanLog[KvArgs] {
    override def logMessage(originalMsg: String, a: KvArgs): String = {
      for (elem <- a.args) {
        val (key, value) = elem
        MDC.put(key, value.toString)
      }
      originalMsg
    }

    override def afterLog(a: KvArgs): Unit = a.args.foreach { case (key, _) => MDC.remove(key) }
  }
}

package mesosphere {

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
   *    // Mixin one of the traits.
   *    class MyClass with ImplicitStrictLogging {....
   *
   *    // Make a log statement with no (empty) context.
   *    logger.info("hello world!")(KvArgs())
   *
   *    // Make a log statement with some context.
   *    logger.info("hello world!")(KvArgs("key1", "value1"))
   *
   *    // Make a log statement with more context.
   *    val moreContext = KvArgs("key1", "value1").and("key2", "value2").and("key3", "value3")
   *    log.info("hello world!")(moreContext.and("key3", "key4"))
   *
   *    // The above statement will log through whatever backend slf4j was tied to statically.
   *    // All the key value parameters in context are populated in MDC just before the log statement is made and are
   *    // deleted immediately after. This happens every time the `logger` object is used to generate a log statement.
   * }}}
   */

  case class KvArgs(args: Traversable[(String, Any)]) {
    /**
     * Helper methods to enable fluent composition of context parameter(s).
     */
    def and(kv: KvArgs): KvArgs = copy(args ++ kv.args)

    def and(k: String, v: String): KvArgs = and(KvArgs(k, v))
  }

  object KvArgs {
    // This defines an empty context.
    // Can be used to make log statements with implicit logger that needs no context.
    def apply(): KvArgs = KvArgs(Traversable())

    def apply(kv: (String, Any)*): KvArgs = KvArgs(kv)

    def apply(key: String, value: Any): KvArgs = KvArgs((key, value))
  }

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
   * Defines `logger` as a lazy value initialized with an underlying `org.slf4j.Logger`
   * named according to the class into which this trait is mixed.
   *
   * Also refer [[com.typesafe.scalalogging.LazyLogging]]
   */
  trait ImplicitLazyLogging {
    @transient
    protected lazy val logger: LoggerTakingImplicit[KvArgs] =
      Logger.takingImplicit[KvArgs](getClass)
  }
}
