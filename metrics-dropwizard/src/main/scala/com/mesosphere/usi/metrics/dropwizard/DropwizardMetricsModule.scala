package com.mesosphere.usi.metrics.dropwizard

import java.lang.management.ManagementFactory

import akka.Done
import akka.actor.ActorRefFactory
import com.codahale.metrics.MetricRegistry
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.metrics.dropwizard.conf.{DataDogApiReporterSettings, DataDogUdpReporterSettings, MetricsSettings}
import com.mesosphere.usi.metrics.dropwizard.reporters.{DataDogAPIReporter, DataDogUDPReporter, StatsDReporter}
import com.typesafe.config.Config

class DropwizardMetricsModule(config: Config) {

  val metricsSettings = MetricsSettings.fromSubConfig(config)

  private lazy val metricNamePrefix = metricsSettings.namePrefix
  private lazy val registry: MetricRegistry = {
    val r = new MetricRegistry
    r.register(s"$metricNamePrefix.jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
    r.register(s"$metricNamePrefix.jvm.gc", new GarbageCollectorMetricSet())
    r.register(s"$metricNamePrefix.jvm.memory", new MemoryUsageGaugeSet())
    r.register(s"$metricNamePrefix.jvm.threads", new ThreadStatesGaugeSet())
    r
  }
  val metrics: Metrics = new DropwizardMetrics(metricsSettings, registry)

  def snapshot(): MetricRegistry = registry

  def start(actorRefFactory: ActorRefFactory): Done = {
    metricsSettings.statsdReporterSettings.foreach(statsdSettings =>
      actorRefFactory.actorOf(StatsDReporter.props(statsdSettings, registry), "StatsDReporter"))

    metricsSettings.dataDogReporterSettings.foreach {
      case s: DataDogUdpReporterSettings => actorRefFactory.actorOf(DataDogUDPReporter.props(s, registry), name = "DataDogUDPReporter")
      case s: DataDogApiReporterSettings => actorRefFactory.actorOf(DataDogAPIReporter.props(s, registry), name = "DataDogAPIReporter")
      case s => throw new IllegalArgumentException(s"Unsupported DataDog reporter typ: $s")
    }
    Done
  }
}
