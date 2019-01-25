package com.mesosphere.usi.persistence.serialization

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
