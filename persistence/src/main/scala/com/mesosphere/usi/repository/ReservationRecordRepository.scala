package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{ReservationId, ReservationRecord}

import scala.concurrent.Future

/**
  * Repository that stores [[ReservationRecord]].
  */
trait ReservationRecordRepository {

  /**
    * Stores the provided [[ReservationRecord]] in the repository.
    * @param record
    * @return
    */
  def store(record: ReservationRecord): Future[Unit]

  /**
    * Retrieves the [[ReservationRecord]] if it exists.
    * @param reservationId
    * @return
    */
  def find(reservationId: ReservationId): Future[Option[ReservationRecord]]

}
