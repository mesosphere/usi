package com.mesosphere.usi.metrics.dummy

import java.time.Clock

import akka.stream.scaladsl.Source
import com.mesosphere.usi.metrics.{ClosureGauge, Counter, Gauge, Meter, Metrics, SettableGauge, Timer}
import com.mesosphere.usi.metrics.{UnitOfMeasurement => DropwizardUnitOfMeasurement}

import scala.concurrent.Future

object DummyMetrics extends Metrics {
  class DummyCounter extends Counter {
    override def increment(): Unit = ()
    override def increment(times: Long): Unit = ()
  }
  class DummyGauge extends SettableGauge {
    override def increment(by: Long): Unit = ()
    override def decrement(by: Long): Unit = ()
    override def value(): Long = 0L
    override def setValue(value: Long): Unit = ()
  }
  class DummyClosureGauge extends ClosureGauge
  class DummyMeter extends Meter {
    override def mark(): Unit = ()
  }
  class DummyTimer extends Timer {
    override def apply[T](f: => Future[T]): Future[T] = f
    override def blocking[T](f: => T): T = f
    override def forSource[T, M](f: => Source[T, M])(implicit clock: Clock): Source[T, M] = f
    override def update(value: Long): Unit = ()
  }

  override def counter(name: String, unit: DropwizardUnitOfMeasurement = DropwizardUnitOfMeasurement.None): Counter =
    new DummyCounter
  override def gauge(name: String, unit: DropwizardUnitOfMeasurement = DropwizardUnitOfMeasurement.None): Gauge =
    new DummyGauge
  override def closureGauge[N](
      name: String,
      currentValue: () => N,
      unit: DropwizardUnitOfMeasurement = DropwizardUnitOfMeasurement.None): ClosureGauge = new DummyClosureGauge
  override def settableGauge(
      name: String,
      unit: DropwizardUnitOfMeasurement = DropwizardUnitOfMeasurement.None): SettableGauge =
    new DummyGauge

  override def meter(name: String): Meter = new DummyMeter
  override def timer(name: String): Timer = new DummyTimer
}
