package com.mesosphere.usi.metrics

import java.time.Clock
import akka.stream.scaladsl.Source
import scala.concurrent.Future

/**
  * Basic trait for all metric types
  */
trait Metric

/**
  * A gauge is an instantaneous measurement of a value e.g. number of offer processed.
  */
trait Gauge extends Metric {
  def value(): Long
  def increment(by: Long = 1): Unit
  def decrement(by: Long = 1): Unit
}

/**
  * A counter is just a gauge for an [[java.util.concurrent.atomic.AtomicLong]] instance. You can increment or decrement
  * its value.
  */
trait Counter extends Metric {
  def increment(): Unit
  def increment(times: Long): Unit
}

/**
  * A settable gauge is like the usual [[Gauge]] but you can also set it's value directly.
  */
trait SettableGauge extends Gauge {
  def setValue(value: Long): Unit
}

trait ClosureGauge extends Metric

/**
  * A meter metric which measures mean throughput and one-, five-, and fifteen-minute exponentially-weighted moving
  * average throughput.
  */
trait Meter extends Metric {
  def mark(): Unit
}

trait TimerAdapter extends Metric {
  def update(value: Long): Unit
}

/**
  * A timer measures both the rate that a particular piece of code is called and the distribution of its duration.
  */
trait Timer extends TimerAdapter {
  def apply[T](f: => Future[T]): Future[T]
  def forSource[T, M](f: => Source[T, M])(implicit clock: Clock = Clock.systemUTC): Source[T, M]
  def blocking[T](f: => T): T

  // value is in nanoseconds
  def update(value: Long): Unit
}

trait Metrics {
  def counter(name: String, unit: UnitOfMeasurement = UnitOfMeasurement.None): Counter
  def gauge(name: String, unit: UnitOfMeasurement = UnitOfMeasurement.None): Gauge
  def closureGauge[N](name: String, currentValue: () => N,
                      unit: UnitOfMeasurement = UnitOfMeasurement.None): ClosureGauge
  def settableGauge(name: String, unit: UnitOfMeasurement = UnitOfMeasurement.None): SettableGauge
  def meter(name: String): Meter
  def timer(name: String): Timer
}

