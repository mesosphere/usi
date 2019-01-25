package com.mesosphere.usi.persistence.storageimpl

import java.util.concurrent.ConcurrentHashMap

import akka.Done
import akka.util.ByteString
import com.mesosphere.usi.persistence.storage.KVStorage

import scala.concurrent.Future

/**
  * In-memory implementation of the key-value storage which doesn't use any persistence
  * and can be used for the development/test purpose.
  */

class InMemoryStorageImpl extends KVStorage {

  private val concurrentHashMap = new ConcurrentHashMap[ByteString, Array[Byte]]() //ByteString due to .equals on arrays

  override def write(key: Array[Byte], value: Array[Byte]): Future[Done] = {
    concurrentHashMap.put(ByteString.fromArray(key), value)
    Future.successful(Done)
  }

  override def read(key: Array[Byte]): Future[Option[Array[Byte]]] = {
    val result = Option(concurrentHashMap.get(ByteString.fromArray(key)))
    Future.successful(result)
  }
}