package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}

import scala.concurrent.Future

/**
  * Repository for [[PodRecord]].
  */
trait PodRecordRepository {

  /**
    * Stores the provided [[PodRecord]] in the repository if it doesn't exist.
    * @param record
    * @return
    */
  def create(record: PodRecord): Future[PodId]

  /**
    * Retrieves the [[PodRecord]] if it exists.
    * @param podId
    * @return Option(PodRecord) if it exists, None otherwise
    */
  def read(podId: PodId): Future[Option[PodRecord]]

  /**
    * Updates the [[PodRecord]] if it exists.
    * @param record
    * @return id of the updated record
    */
  def update(record: PodRecord): Future[PodId]

  /**
    * Deletes the [[PodRecord]] if it exists.
    * @param record
    * @return id of the deleted record
    */
  def delete(record: PodRecord): Future[PodId]

}
