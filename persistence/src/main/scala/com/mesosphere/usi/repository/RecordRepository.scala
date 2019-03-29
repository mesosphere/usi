package com.mesosphere.usi.repository

import scala.concurrent.Future

trait RecordRepository {

  type Record

  type RecordId

  /**
    * Stores the provided record in the repository if it doesn't exist.
    * If the record is already there, resulting future will be failed with a [[RecordAlreadyExistsException]].
    * @param record
    * @return id of the provided record
    */
  def store(record: Record): Future[RecordId]

  /**
    * Retrieves all the existing records (if any).
    * @return Map containing all the current pod records. Can be empty if there are no pod records.
    */
  def readAll(): Future[Map[RecordId, Record]]

  /**
    * Deletes the record if it exists. If the record is missing,
    * * the future will be failed with an [[RecordNotFoundException]].
    *
    * @param recordId
    * @return Unit either if the node delete was successful OR if the node did not exist.
    */
  def delete(recordId: RecordId): Future[Unit]
}

case class RecordAlreadyExistsException(id: String) extends RuntimeException(s"record with id $id already exists.")
case class RecordNotFoundException(id: String) extends RuntimeException(s"record with id $id doesn't exist.")
