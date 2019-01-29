package mesosphere.mesos.conf

import com.typesafe.config.Config

import scala.concurrent.duration._

case class MesosClientSettings(conf: Config) {
  val master: String = conf.getString("connection.masterUrl")
  val redirectRetries: Int = conf.getInt("connection.redirectRetries")
  // we want FiniteDuration, the conversion is needed to achieve that
  val idleTimeout: FiniteDuration = Duration.fromNanos(conf.getDuration("connection.idleTimeout").toNanos)

  val sourceBufferSize: Int = conf.getInt("backPressure.sourceBufferSize")
}
