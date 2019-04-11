package com.mesosphere.usi.repository

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

trait RecordRepository {

  type Record

  type RecordId

  /**
    * Flow that deletes the [[Record]] for given [[RecordId]] if it exists. If the record is missing, this is a no-op.
    */
  def deleteFlow: Flow[RecordId, Unit, NotUsed]

  /**
    * Flow that consumes a [[Record]] and emits a corresponding [[RecordId]] after a successful create/update.
    */
  def storeFlow: Flow[Record, RecordId, NotUsed]

  /**
    * Source that retrieves all the existing records (if any) at the time of materialization. Creates a map containing
    * all the current records on every materialization. Can be an empty map if there are no records.
    * Source completes after emitting the current snapshot.
    */
  def readAll(): Source[Map[RecordId, Record], NotUsed]
}
