package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}

import scala.concurrent.Future

/**
  * Repository that stores [[PodRecord]].
  */
trait PodRecordRepository {

  /**
    * Stores the provided [[PodRecord]] in the repository.
    * @param record
    * @return
    */
  def store(record: PodRecord): Future[Unit]

  /**
    * Retrieves the [[PodRecord]] if it exists.
    * @param podId
    * @return
    */
  def find(podId: PodId): Future[Option[PodRecord]]

}
