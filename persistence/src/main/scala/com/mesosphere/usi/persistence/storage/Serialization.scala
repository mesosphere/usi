package com.mesosphere.usi.persistence.storage

import com.mesosphere.usi.persistence.SerializationFormat

object Serialization {

  def toBinary[T : SerializationFormat](o: T): Array[Byte] = {
    val format = implicitly[SerializationFormat[T]]
    format.writes(o)
  }

  def fromBinary[T : SerializationFormat](blob: Array[Byte]): T = {
    val format = implicitly[SerializationFormat[T]]
    format.reads(blob)
  }

}

