package com.mesosphere.usi.metrics

import java.time.Clock
import akka.stream.scaladsl.Source
import scala.concurrent.Future

trait Metric

trait Counter extends Metric {
  def increment(): Unit
  def increment(times: Long): Unit
}

trait Gauge extends Metric {
  def value(): Long
  def increment(by: Long = 1): Unit
  def decrement(by: Long = 1): Unit
}

trait SettableGauge extends Gauge {
  def setValue(value: Long): Unit
}

trait ClosureGauge extends Metric

trait MinMaxCounter extends Metric {
  def increment(): Unit
  def increment(times: Long): Unit
  def decrement(): Unit
  def decrement(times: Long): Unit
}

trait Meter extends Metric {
  def mark(): Unit
}

trait TimerAdapter extends Metric {
  def update(value: Long): Unit
}

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

