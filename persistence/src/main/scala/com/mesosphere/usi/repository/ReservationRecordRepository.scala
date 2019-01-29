package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{ReservationId, ReservationRecord}

import scala.concurrent.Future

/**
  * Repository for [[ReservationRecord]].
  */
trait ReservationRecordRepository {

  /**
    * Stores the provided [[ReservationRecord]] in the repository if it doesn't exist.
    * @param record
    * @return
    */
  def create(record: ReservationRecord): Future[ReservationId]

  /**
    * Retrieves the [[ReservationRecord]] if it exists.
    * @param podId
    * @return Option(ReservationRecord) if it exists, None otherwise
    */
  def read(podId: ReservationId): Future[Option[ReservationRecord]]

  /**
    * Updates the [[ReservationRecord]] if it exists.
    * @param record
    * @return id of the updated record
    */
  def update(record: ReservationRecord): Future[ReservationId]

  /**
    * Deletes the [[ReservationRecord]] if it exists.
    * @param record
    * @return id of the deleted record
    */
  def delete(record: ReservationRecord): Future[ReservationId]

}
