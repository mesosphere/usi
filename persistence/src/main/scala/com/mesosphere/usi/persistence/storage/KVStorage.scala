package com.mesosphere.usi.persistence.storage

import akka.Done

import scala.concurrent.Future

/**
  * Base trait that supports read/write operations.
  *
  * Implementations supposed to be wrappers around an actual database.
  *
  * Underlying storage MUST provide happens-before guarantees for a single key operation, e.g.
  * if a write to a key A happened before the read of that key, then the read MUST return the written value.
  *
  * @see [[com.mesosphere.usi.persistence.storageimpl.InMemoryStorageImpl]] for an in-memory implementation
  */

trait KVStorage {

  /**
    * Writes the value at the provided key. Checking if value already exists is not requierd.
    *
    * @param key
    * @param value
    * @return
    */
  def write(key: Array[Byte], value: Array[Byte]): Future[Done]

  /**
    * Reads the value at the provided key. If value doesn't exist, returns None.
    * @param key
    * @return
    */
  def read(key: Array[Byte]): Future[Option[Array[Byte]]]

}

