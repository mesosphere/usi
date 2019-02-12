package com.mesosphere.usi.helloworld

import com.mesosphere.usi.core.Scheduler
import com.typesafe.scalalogging.StrictLogging
import com.mesosphere._
import net.logstash.logback.marker.Markers._

/**
  * A demonstration of USI features.
  */
object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val _ = Scheduler.connect("asd")
    logger.withMarker(append("hello", "world")).info("hello main")
    test()
  }

  def test(): Unit = {
    logger.info("hello test")
  }
}
