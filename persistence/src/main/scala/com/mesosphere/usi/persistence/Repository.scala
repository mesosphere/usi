package com.mesosphere.usi.persistence

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.{PodRecord, ReservationRecord}

import scala.concurrent.Future

trait Repository {

  def storePodRecord(record: PodRecord): Future[Done]

  def storeReservationRecord(record: ReservationRecord): Future[Done]

  def podRecords(fromSequenceNr: Long = 0, toSequenceNr: Long = Long.MaxValue): Source[PodRecord, NotUsed]

  def reservationRecords(fromSequenceNr: Long = 0, toSequenceNr: Long = Long.MaxValue): Source[ReservationRecord, NotUsed]

}