package com.mesosphere.usi.repository

import scala.concurrent.Future

trait RecordRepository[Record, RecordId] {

  /**
    * Stores the provided record in the repository if it doesn't exist.
    * If the record is already there, resulting future will be failed with a [[RecordAlreadyExistsException]].
    * @param record
    * @return id of the provided record
    */
  def create(record: Record): Future[RecordId]

  /**
    * Retrieves the record if it exists.
    * @param recordId
    * @return Option(record) if it exists, None otherwise
    */
  def read(recordId: RecordId): Future[Option[Record]]

  /**
    * Updates the record if it exists. If the record is missing,
    * the future will be failed with an [[RecordNotFoundException]].
    *
    * @param record
    * @return id of the updated record
    */
  def update(record: Record): Future[RecordId]

  /**
    * Deletes the record if it exists. If the record is missing,
    * * the future will be failed with an [[RecordNotFoundException]].
    *
    * @param record
    * @return id of the deleted record
    */
  def delete(record: Record): Future[RecordId]
}

case class RecordAlreadyExistsException(id: String) extends RuntimeException(s"record with id $id already exists.")
case class RecordNotFoundException(id: String) extends RuntimeException(s"record with id $id doesn't exist.")
