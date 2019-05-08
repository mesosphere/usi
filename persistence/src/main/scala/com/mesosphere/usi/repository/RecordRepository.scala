package com.mesosphere.usi.repository

import akka.Done
import scala.concurrent.Future

/*
 * Order of execution must be respected! If a store for record A is invoked, then delete for record A is invoked, in
 * serial without waiting for the completion of each returned future, then the implementation MUST apply these
 * operations in order
 */
trait RecordRepository {

  type Record

  type RecordId

  /**
    * Deletes the record specified, returns completed future when this operation is confirmed to be applied
    *
    * Note the guarantee described for [[RecordRepository]]
    */
  def delete(record: RecordId): Future[Done]

  /**
    * Store the record specified, returns completed future when this operation is confirmed to be applied
    *
    * Note the guarantee described for [[RecordRepository]]
    */
  def store(record: Record): Future[Done]

  /**
    * Source that retrieves all the existing records (if any) at the time of materialization. Creates a map containing
    * all the current records on every materialization. Can be an empty map if there are no records.
    * Source completes after emitting the current snapshot.
    */
  def readAll(): Future[Map[RecordId, Record]]
}
