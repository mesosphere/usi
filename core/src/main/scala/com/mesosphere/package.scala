package com

import com.typesafe.scalalogging.CanLog
import com.typesafe.scalalogging.LoggerTakingImplicit
import org.slf4j.MDC
import scala.language.implicitConversions

package object mesosphere {

  type KvArgs = Traversable[(String, Any)]

  /**
   * By default, we use an empty context for all loggers. This is needed where we have a class/object
   * level implicit logger but it may not always be needed/required to have an implicit [[KvArgs]] in scope.
   */
  implicit val emptyKvArgs : KvArgs = Traversable()

  implicit def tupleToTraversable(kv: (String, Any)): KvArgs = Traversable(kv)

  implicit final class KvArgsOps(val self : KvArgs) extends AnyVal {
    /**
     * Helper methods to enable fluent composition of context parameter(s).
     * E.g.:
     *   log1.ctx.and("key1", "value1").and(log2.ctx)
     */
    def and(kv : KvArgs): KvArgs = self ++ kv
    def and(k : String, v : String): KvArgs = self and ((k, v))
  }

  /**
   * Evidence needed for [[LoggerTakingImplicit]] of type [[KvArgs]]
   */
  implicit case object CanLogKvArgs extends CanLog[KvArgs] {
    override def logMessage(originalMsg: String, a: KvArgs): String = {
      for (elem <- a) {
        MDC.put(elem._1, elem._2.toString)
      }
      originalMsg
    }

    override def afterLog(a: KvArgs): Unit = a.foreach(x => MDC.remove(x._1))
  }

  /**
   * Enable calling logger methods directly on the tuple object.
   * Note that we SHOULD NEVER override or re-implement the `info` or `error` methods as it may interfere
   * with the caller context.
   */
  implicit def tupleToLogger(
    t : (LoggerTakingImplicit[KvArgs], KvArgs)
  ) : LoggerTakingImplicit[KvArgs] = t._1

  /**
   * Used to add context parameters to a simple [[LoggerTakingImplicit]] instance
   */
  implicit class LoggerImplicitOps(val logger: LoggerTakingImplicit[KvArgs]) extends AnyVal {
    def withCtx(key: String, value: Any): (LoggerTakingImplicit[KvArgs], KvArgs) = (logger, (key, value))
    def withCtx(tag: KvArgs): (LoggerTakingImplicit[KvArgs], KvArgs) = (logger, tag)
  }

  /**
   * Used to add context parameters to a tuple containing [[LoggerTakingImplicit]] and [[KvArgs]] instance
   */
  implicit final class LoggerImplicitConverter(
    val self: (LoggerTakingImplicit[KvArgs], KvArgs)
  ) extends AnyVal {

    implicit def ctx : KvArgs = self._2

    def withCtx(key: String, value: Any): (LoggerTakingImplicit[KvArgs], KvArgs) = {
      (self._1, self._2 ++ tupleToTraversable((key, value)))
    }

    def withCtx(newContext: KvArgs): (LoggerTakingImplicit[KvArgs], KvArgs) = {
      (self._1, self._2 ++ newContext)
    }
  }
}
