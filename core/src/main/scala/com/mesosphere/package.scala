package com

import com.typesafe.scalalogging.CanLog
import com.typesafe.scalalogging.LoggerTakingImplicit
import org.slf4j.MDC
import scala.language.implicitConversions

package object mesosphere {

  type KvArgs = Traversable[(String, Any)]

  implicit val emptyKvArgs : KvArgs = Traversable()

  implicit def tupleToTraversable(kv: (String, Any)): KvArgs = Traversable(kv)

  implicit final class KeyValueOrgOps(val self : KvArgs) extends AnyVal {
    def and(kv : KvArgs): KvArgs = self ++ kv
    def and(k : String, v : String): KvArgs = self and ((k, v))
  }

  implicit case object CanLogKvArgs extends CanLog[KvArgs] {
    override def logMessage(originalMsg: String, a: KvArgs): String = {
      for (elem <- a) {
        MDC.put(elem._1, elem._2.toString)
      }
      originalMsg
    }

    override def afterLog(a: KvArgs): Unit = a.foreach(x => MDC.remove(x._1))
  }

  // This is needed so that we can call logger methods directly on the tuple.
  implicit def tupleToLogger(
    t : (LoggerTakingImplicit[KvArgs], KvArgs)
  ) : LoggerTakingImplicit[KvArgs] = t._1

  implicit class LoggerImplicitOps(val logger: LoggerTakingImplicit[KvArgs]) extends AnyVal {
    def ctx(tag: KvArgs): (LoggerTakingImplicit[KvArgs], KvArgs) = (logger, tag)
  }

  implicit final class LoggerImplicitConverter(
    val self: (LoggerTakingImplicit[KvArgs], KvArgs)
  ) extends AnyVal {

    implicit def getCtx : KvArgs = self._2

    def ctx(newContext: KvArgs): (LoggerTakingImplicit[KvArgs], KvArgs) = {
      (self._1, self._2 ++ newContext)
    }
  }
}
