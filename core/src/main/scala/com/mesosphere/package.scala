package com

import com.typesafe.scalalogging.Logger
import net.logstash.logback.marker.LogstashMarker
import org.slf4j.Marker

package object mesosphere {

  implicit final class LogstashMarkerOps(val self: LogstashMarker) extends AnyVal {
    def withMarker(marker: Marker): LogstashMarker = self.and[LogstashMarker](marker)
  }

  implicit class LoggerOps(val logger : Logger) extends AnyVal {
    def withMarker(marker: Marker): (Logger, Marker) = (logger, marker)
  }

  implicit final class UsiLoggerConverter(val self : (Logger, Marker)) extends AnyVal {
    def withMarker(markers: Marker*): (Logger, Marker) = {
      markers.foreach(self._2.add)
      self
    }
    def info(msg: String): Unit = self._1.info(self._2, msg)

    def info(msg : String, y : Any) = self._1.info(self._2, msg, y)
  }
}
