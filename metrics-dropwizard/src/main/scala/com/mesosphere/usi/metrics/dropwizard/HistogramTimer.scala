package com.mesosphere.usi.metrics.dropwizard

import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import java.time.{Clock, Duration, Instant}

import com.mesosphere.usi.async.ExecutionContexts
import com.mesosphere.usi.metrics.{Timer, TimerAdapter}

import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * Akka Graph Stage that measures the time from the start of the stream until the end of it, where start is defined as
  * the instant the stream is materialized
  */
class TimedStage[T](timer: TimerAdapter, clock: Clock) extends GraphStage[FlowShape[T, T]] {
  val in = Inlet[T]("timer.in")
  val out = Outlet[T]("timer.out")

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    val logic = new GraphStageLogic(shape) {
      private val start: Instant = clock.instant

      setHandler(
        in,
        new InHandler {
          @scala.throws[Exception](classOf[Exception])
          override def onPush(): Unit = push(out, grab(in))

          @scala.throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = {
            timer.update(Duration.between(start, clock.instant).toNanos)
            super.onUpstreamFinish()
          }

          @scala.throws[Exception](classOf[Exception])
          override def onUpstreamFailure(ex: Throwable): Unit = {
            timer.update(Duration.between(start, clock.instant).toNanos)
            super.onUpstreamFailure(ex)
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          @scala.throws[Exception](classOf[Exception])
          override def onPull(): Unit = pull(in)
        }
      )
    }
    logic
  }

  override def shape: FlowShape[T, T] = FlowShape.of(in, out)
}

case class HistogramTimer(timer: TimerAdapter) extends Timer {

  override def apply[T](f: => Future[T]): Future[T] = {
    val start = System.nanoTime()
    val future =
      try {
        f
      } catch {
        case NonFatal(e) =>
          timer.update(System.nanoTime() - start)
          throw e
      }
    future.onComplete(_ => timer.update(System.nanoTime() - start))(ExecutionContexts.callerThread)
    future
  }

  override def forSource[T, M](f: => Source[T, M])(implicit clock: Clock = Clock.systemUTC): Source[T, M] = {
    val src = f
    val flow = Flow.fromGraph(new TimedStage[T](timer, clock))
    val transformed = src.via(flow)
    transformed
  }

  override def blocking[T](f: => T): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      timer.update(System.nanoTime() - start)
    }
  }

  override def update(value: Long): Unit = timer.update(value)
}
