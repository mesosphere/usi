package com.mesosphere.usi.core

import akka.NotUsed

import akka.stream.scaladsl.{Flow, Source}

object Scheduler {
  type SpecState = (SpecSnapshot, Source[SpecUpdate, NotUsed])
  type StatusState = (StatusSnapshot, Source[StatusUpdate, NotUsed])

  // Materialized value should probably be a Future containing Mesos master connection info etc.
  val usi: Flow[SpecState, StatusState, NotUsed] = ???
}
