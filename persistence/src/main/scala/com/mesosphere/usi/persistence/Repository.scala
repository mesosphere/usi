package com.mesosphere.usi.persistence

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.{PodRecord, ReservationRecord}

import scala.concurrent.Future


/**
  * Base trait for storing records which are used by the Scheduler.
  *
  * Usually it's enough to implement a [[storage.KVStorage]] trait and plug it into the [[KVRepositoryImpl]],
  * however if the target database supports fast range reads, the implementation can
  * override a [[Repository]] directly.
  *
  */
trait Repository {

  /**
    * Stores the provided pod record
    * @param record
    * @return
    */
  def storePodRecord(record: PodRecord): Future[Done]

  /**
    * Stores the provided reservation record
    * @param record
    * @return
    */
  def storeReservationRecord(record: ReservationRecord): Future[Done]

  /**
    * Retrieves a range of pod records based on their sequence numbers
    * @param fromSequenceNr start reading reconds from here
    * @param toSequenceNr stop reading when reached this seqNr (inclusive)
    * @return
    */
  def podRecords(fromSequenceNr: Long = 0, toSequenceNr: Long = Long.MaxValue): Source[PodRecord, NotUsed]

  /**
    * Retrieves a range of pod records based on their sequence numbers
    * @param fromSequenceNr start reading reconds from here
    * @param toSequenceNr stop reading when reached this seqNr (inclusive)
    * @return
    */
  def reservationRecords(fromSequenceNr: Long = 0, toSequenceNr: Long = Long.MaxValue): Source[ReservationRecord, NotUsed]

}