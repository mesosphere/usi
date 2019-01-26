package com.mesosphere.usi.metrics.dropwizard.conf

import com.typesafe.config.ConfigFactory
import org.scalatest.{GivenWhenThen, Inside, Matchers, OptionValues, WordSpec}

import scala.concurrent.duration._

class MetricsSettingsTest extends WordSpec
  with Matchers
  with GivenWhenThen
  with Inside
  with OptionValues {

  "MetricsSettings" should {
    "parse reference.conf correctly" in {
      When("configuration is loaded")
      val config = ConfigFactory.load()

      Then("it can be successfully parsed")
      val settings = MetricsSettings.fromSubConfig(config.getConfig("usi.metrics"))

      And("values are set correctly")
      settings.namePrefix shouldBe "usi"
      val historgramSettings = settings.historgramSettings
      historgramSettings.reservoirHighestTrackableValue shouldBe 3600000000000L
      historgramSettings.reservoirSignificantDigits shouldBe 3
      historgramSettings.reservoirResetPeriodically shouldBe true
      historgramSettings.reservoirResettingInterval shouldBe 5.seconds
      historgramSettings.reservoirResettingChunks shouldBe 0
      settings.dataDogReporterSettings shouldBe None
      settings.statsdReporterSettings shouldBe None
    }

    "parse a configuration with an enabled StatsD reporter" in {
      Given("a configuration with statsd reporter")
      val conf =
        """
          |usi.metrics {
          |  name-prefix: "usi"
          |  histogram {
          |    reservoir-highest-trackable-value: 3600000000000
          |    reservoir-significant-digits: 3
          |    reservoir-reset-periodically: true
          |    reservoir-resetting-interval: 5000ms
          |    reservoir-resetting-chunks: 0
          |  }
          |
          |  statsd-reporter: on
          |  # StatsD reporter
          |    reporters.statsd {
          |        host: "localhost"
          |        port: 8125
          |        transmission-interval: 10000ms
          |    }
          |
          |  datadog-reporter: off
          |}
        """.stripMargin

      When("configuration is loaded")
      val config = ConfigFactory.parseString(conf)

      Then("it can be successfully parsed")
      val settings = MetricsSettings.fromSubConfig(config.getConfig("usi.metrics"))

      And("StatsD reporter settings are set correctly")
      inside(settings.statsdReporterSettings.value) {
        case StatsdReporterSettings(host, port, transmissionInterval) =>
          host shouldBe "localhost"
          port shouldBe 8125
          transmissionInterval shouldBe 10.seconds
      }
    }

    "parse a configuration with an enabled DataDog API reporter" in {
      Given("a configuration with statsd reporter")
      val conf =
        """
          |usi.metrics {
          |  name-prefix: "usi"
          |  histogram {
          |    reservoir-highest-trackable-value: 3600000000000
          |    reservoir-significant-digits: 3
          |    reservoir-reset-periodically: true
          |    reservoir-resetting-interval: 5000ms
          |    reservoir-resetting-chunks: 0
          |  }
          |
          |  statsd-reporter: off
          |
          |  datadog-reporter: on
          |  # DataDog settings
          |  reporters.datadog {
          |    # host:
          |    # port:
          |    protocol: "api"
          |    api-key: "FAKE-API-KEY"
          |    transmission-interval: 10000ms
          |  }
          |}
        """.stripMargin

      When("configuration is loaded")
      val config = ConfigFactory.parseString(conf)

      Then("it can be successfully parsed")
      val settings = MetricsSettings.fromSubConfig(config.getConfig("usi.metrics"))

      And("DataDog API reporter settings are set correctly")
      inside(settings.dataDogReporterSettings.value) {
        case DataDogApiReporterSettings(apiKey, transmissionInterval) =>
          apiKey shouldBe "FAKE-API-KEY"
          transmissionInterval shouldBe 10.seconds
      }
    }

    "parse a configuration with an enabled DataDog UDP reporter" in {
      Given("a configuration with statsd reporter")
      val conf =
        """
          |usi.metrics {
          |  name-prefix: "usi"
          |  histogram {
          |    reservoir-highest-trackable-value: 3600000000000
          |    reservoir-significant-digits: 3
          |    reservoir-reset-periodically: true
          |    reservoir-resetting-interval: 5000ms
          |    reservoir-resetting-chunks: 0
          |  }
          |
          |  statsd-reporter: off
          |
          |  datadog-reporter: on
          |  # DataDog settings
          |  reporters.datadog {
          |    host: localhost
          |    port: 8080
          |    protocol: "udp"
          |    # api-key:
          |    transmission-interval: 10000ms
          |  }
          |}
        """.stripMargin

      When("configuration is loaded")
      val config = ConfigFactory.parseString(conf)

      Then("it can be successfully parsed")
      val settings = MetricsSettings.fromSubConfig(config.getConfig("usi.metrics"))

      And("DataDog API reporter settings are set correctly")
      inside(settings.dataDogReporterSettings.value) {
        case DataDogUdpReporterSettings(host, port, transmissionInterval) =>
          host shouldBe "localhost"
          port shouldBe 8080
          transmissionInterval shouldBe 10.seconds
      }
    }
  }
}
