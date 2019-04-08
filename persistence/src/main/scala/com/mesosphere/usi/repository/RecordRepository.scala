package com.mesosphere.usi.repository

import akka.NotUsed
import akka.stream.scaladsl.Flow
import scala.concurrent.Future

trait RecordRepository {

  type Record

  type RecordId

  /**
    * Create/update the provided record in the repository.
    * @param record
    * @return id of the provided record
    */
  def store(record: Record): Future[RecordId]
  def storeFlow: Flow[Record, RecordId, NotUsed]

  /**
    * Retrieves all the existing records (if any).
    * @return Map containing all the current pod records. Can be empty if there are no pod records.
    */
  def readAll(): Future[Map[RecordId, Record]]

  /**
    * Deletes the record if it exists. If the record is missing, this is a no-op.
    * @param recordId
    * @return Unit either if the node delete was successful OR if the node did not exist.
    */
  def delete(recordId: RecordId): Future[Unit]
  def deleteFlow: Flow[RecordId, Unit, NotUsed]
}
