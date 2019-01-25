package com.mesosphere.usi.persistence.storage

import akka.Done

import scala.concurrent.Future

trait KVStorage {

  def write(key: Array[Byte], value: Array[Byte]): Future[Done]

  def read(key: Array[Byte]): Future[Option[Array[Byte]]]

}

