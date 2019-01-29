package mesosphere.mesos.conf

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
  * Configuring mesos v1 client
  *
  * @param master The URL of the Mesos master
  * @param sourceBufferSize Buffer size of the mesos source im messages
  * @param redirectRetries Number of retries to follow mesos master redirect
  * @param idleTimeout Time in seconds between two processed elements exceeds the provided timeout then the connection to mesos
  *                    is interrupted. Is usually set to approx. 5 hear beats.
  */
case class MesosClientConf(conf: Config) {
  val master: String = conf.getString("connection.masterUrl")
  val redirectRetries: Int = conf.getInt("connection.redirectRetries")
  // we want FiniteDuration, the conversion is needed to achieve that
  val idleTimeout: FiniteDuration = Duration.fromNanos(conf.getDuration("connection.idleTimeout").toNanos)

  val sourceBufferSize: Int = conf.getInt("backPressure.sourceBufferSize")
}
