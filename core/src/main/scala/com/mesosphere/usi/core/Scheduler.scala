package com.mesosphere.usi.core

import akka.NotUsed

import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.usi.models._

object Scheduler {
  type Specs = (SpecsSnapshot, Source[SpecChange, NotUsed])
  type Statuses = (StatusSnapshot, Source[StatusChange, NotUsed])


  // Materialized value should probably be a Future containing Mesos master connection info etc.
  val usi: Flow[Specs, Statuses, NotUsed] = Flow[Specs]
    .take(1)
    .flatMapConcat { case (specsSnapshot, specChanges) =>
      Source.single(specsSnapshot).concat(specChanges)
    }
    .statefulMapConcat { () =>
      val schedulerLogic = new SchedulerLogic

      schedulerLogic.receive(_)
    }
    .prefixAndTail(1).map { case (Seq(statusSnapshot), statusChanges) =>
      statusSnapshot.asInstanceOf[StatusSnapshot] -> statusChanges.asInstanceOf[Source[StatusChange, NotUsed]]
    }
}
