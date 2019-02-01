package com.mesosphere.usi.metrics.dropwizard.conf

import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import net.ceedubs.ficus.Ficus.{finiteDurationReader, toFicusConfig}

case class HistorgramSettings(reservoirHighestTrackableValue: Long = 3600000000000L,
                              reservoirSignificantDigits: Int = 3,
                              reservoirResetPeriodically: Boolean = true,
                              reservoirResettingInterval: FiniteDuration = 5.seconds,
                              reservoirResettingChunks: Int = 0) {
  require(reservoirHighestTrackableValue > 1L, "reservoir-highest-trackable-value: should be > 1")
  require(reservoirSignificantDigits >= 0 && reservoirSignificantDigits <= 5,
          "reservoir-significant-digits should be >= 0 and <= 5")
  require(reservoirResettingInterval > Duration.Zero, "reservoir-resetting-interval should be > 0")
  require(reservoirResettingChunks == 0 || reservoirResettingChunks >= 2,
          "reservoir-resetting-chunks should be == 0 or >= 2")
}

object HistorgramSettings {
  def fromSubConfig(c: Config) = apply(
    c.getLong("reservoir-highest-trackable-value"),
    c.getInt("reservoir-significant-digits"),
    c.getBoolean("reservoir-reset-periodically"),
    c.as[FiniteDuration]("reservoir-resetting-interval"),
    c.getInt("reservoir-resetting-chunks")
  )
}

case class StatsdReporterSettings(host: String, port: Int, transmissionInterval: FiniteDuration = 10.seconds) {
  require(host.nonEmpty, "StatsD reporter host should be defined")
  require(port > 0, "StatsD reporter port should be defined")
  require(transmissionInterval > Duration.Zero, "transmitiona-interavl should be > 0")
}

object StatsdReporterSettings {
  def fromSubConfig(c: Config) = apply(
    c.getString("host"),
    c.getInt("port"),
    c.as[FiniteDuration]("transmission-interval")
  )
}

sealed trait DataDogReporterSettings

case class DataDogUdpReporterSettings(host: String, port: Int, transmissionInterval: FiniteDuration = 10.seconds)
    extends DataDogReporterSettings {
  require(host.nonEmpty, "DataDog reporter host should be defined")
  require(port > 0, "DataDog reporter port should be > 0")
  require(transmissionInterval > Duration.Zero, "transmitiona-interavl should be > 0")
}

case class DataDogApiReporterSettings(apiKey: String, transmissionInterval: FiniteDuration = 10.seconds)
    extends DataDogReporterSettings {
  require(transmissionInterval > Duration.Zero, "transmitiona-interavl should be > 0")
}

object DataDogReporterSettings {

  def fromSubConfig(c: Config) = c.getString("protocol") match {
    case "udp" =>
      DataDogUdpReporterSettings(
        c.getString("host"),
        c.getInt("port"),
        c.as[FiniteDuration]("transmission-interval")
      )
    case "api" =>
      DataDogApiReporterSettings(
        c.getString("api-key"),
        c.as[FiniteDuration]("transmission-interval")
      )
    case p =>
      throw new IllegalArgumentException(
        s"Protocol $p for the DataDog reporter is not supported. Please use 'udp' or 'api' instead")
  }
}

case class MetricsSettings(namePrefix: String,
                           historgramSettings: HistorgramSettings,
                           statsdReporterSettings: Option[StatsdReporterSettings],
                           dataDogReporterSettings: Option[DataDogReporterSettings]) {
  require(namePrefix.nonEmpty, "name-prefix should not be empty")
}

object MetricsSettings {

  def fromSubConfig(c: Config): MetricsSettings = apply(
    c.getString("name-prefix"),
    HistorgramSettings.fromSubConfig(c.getConfig("histogram")),
    statsdReporterSettings =
      if (c.getBoolean("statsd-reporter"))
        Some(StatsdReporterSettings.fromSubConfig(c.getConfig("reporters.statsd")))
      else None,
    dataDogReporterSettings =
      if (c.getBoolean("datadog-reporter"))
        Some(DataDogReporterSettings.fromSubConfig(c.getConfig("reporters.datadog")))
      else None
  )
}
