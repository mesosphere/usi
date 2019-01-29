package com.mesosphere.usi.core

import akka.NotUsed

import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.usi.models._

object Scheduler {
  type Specs = (SpecsSnapshot, Source[SpecChange, NotUsed])
  type Statuses = (StatusSnapshot, Source[StatusChange, NotUsed])

}
