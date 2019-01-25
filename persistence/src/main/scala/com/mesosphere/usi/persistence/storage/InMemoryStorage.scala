package com.mesosphere.usi.persistence.storage

import java.util.concurrent.ConcurrentHashMap

import akka.Done
import akka.util.ByteString

import scala.concurrent.Future

class InMemoryStorage extends KVStorage {

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