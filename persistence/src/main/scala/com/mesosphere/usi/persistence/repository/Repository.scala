package com.mesosphere.usi.persistence.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord, ReservationId, ReservationRecord}

import scala.concurrent.Future

/**
  * Base trait for storing records which are used by the Scheduler.
  *
  */
trait Repository {

  /**
    * Stores the provided pod record
    * @param record
    * @return
    */
  def storePodRecord(record: PodRecord): Future[Unit]

  /**
    * Stores the provided reservation record
    * @param record
    * @return
    */
  def storeReservationRecord(record: ReservationRecord): Future[Unit]

  /**
    * Retrieves a range of pod records based on their sequence numbers
    * @param id id of the corresponding pod
    * @return
    */
  def findPodRecord(id: PodId): Future[Option[PodRecord]]

  /**
    * Tries to find a reservation record given the reservation id
    * @param id id of the corresponding reservation
    * @return
    */
  def findReservationRecord(id: ReservationId): Future[Option[ReservationRecord]]

}
