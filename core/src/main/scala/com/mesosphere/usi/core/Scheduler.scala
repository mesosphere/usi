package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.mesosphere.usi._

object Scheduler {
  type Specs = (SpecsSnapshot, Source[SpecChange, NotUsed])
  type Statuses = (StatusSnapshot, Source[StatusChange, NotUsed])
}
