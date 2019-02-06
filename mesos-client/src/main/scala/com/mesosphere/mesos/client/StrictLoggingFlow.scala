package com.mesosphere.mesos.client

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging

trait StrictLoggingFlow extends StrictLogging {
  protected def debug[T](prefix: String): Flow[T, T, NotUsed] = Flow[T].map { e =>
    logger.debug(s"$prefix$e"); e
  }
  protected def info[T](prefix: String): Flow[T, T, NotUsed] = Flow[T].map { e =>
    logger.info(s"$prefix$e"); e
  }
  protected def warn[T](prefix: String): Flow[T, T, NotUsed] = Flow[T].map { e =>
    logger.warn(s"$prefix$e"); e
  }
  protected def error[T](prefix: String): Flow[T, T, NotUsed] = Flow[T].map { e =>
    logger.error(s"$prefix$e"); e
  }

}
