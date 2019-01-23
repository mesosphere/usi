package com.mesosphere.usi.core

import akka.NotUsed

import akka.stream.scaladsl.{Flow, Source}

object Scheduler {
  type Specs = (SpecsSnapshot, Source[SpecChange, NotUsed])
  type Statuses = (StatusSnapshot, Source[StatusChange, NotUsed])

  class SchedulerLogic {
    var podSpecs: Map[PodId, PodSpec] = Map.empty
    var reservationSpecs: Map[ReservationId, ReservationSpec] = Map.empty

    def mockStatusFor(podSpec: PodSpec): PodStatus = {
      podSpec.goal match {
        case Goal.Running =>
          PodStatus(podSpec.id, taskStatuses = Map(podSpec.id -> MesosTaskStatus.TASK_RUNNING))
        case Goal.Terminal =>
          PodStatus(podSpec.id, taskStatuses = Map(podSpec.id -> MesosTaskStatus.TASK_KILLED))
      }
    }

    def receive(msg: SpecMessage): List[StatusMessage] = msg match {
      case SpecsSnapshot(podSpecs, reservationSpecs) =>
        this.podSpecs = podSpecs.map { p => p.id -> p }(collection.breakOut)
        this.reservationSpecs = reservationSpecs.map { r => r.id -> r }(collection.breakOut)

        List(
          StatusSnapshot(
            podStatuses = this.podSpecs.values.map(mockStatusFor)(collection.breakOut),
            reservationStatuses = Nil
          ))
      case ReservationSpecChange(id, _) =>
        ???
      case PodSpecChange(id, None) =>
        this.podSpecs -= id

        List(PodStatusChange(id, None))
      case PodSpecChange(id, Some(podSpec)) =>
        this.podSpecs += (id -> podSpec)

        List(PodStatusChange(id, Some(mockStatusFor(podSpec))))
    }
  }


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
