package com.mesosphere.usi.persistence.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.mesosphere.usi.core.models.{PodRecord, ReservationRecord}

import scala.reflect.ClassTag

object SerializationFormats {

  /**
    * Java serialization for in-memory tests. Don't use in production!
    */
  private def javaSerializationObjectFormat[T: ClassTag] = new SerializationFormat[T] {

    override def writes(o: T): Array[Byte] = {
      val outputStream = new ByteArrayOutputStream
      val objectOutputStream  = new ObjectOutputStream(outputStream)
      objectOutputStream.writeObject(o)
      objectOutputStream.flush()
      objectOutputStream.close()
      outputStream.toByteArray
    }

    override def reads(blob: Array[Byte]): T = {
      val is = new ByteArrayInputStream(blob)
      val objectInputStream = new ObjectInputStream(is)
      val record = objectInputStream.readObject().asInstanceOf[T]
      objectInputStream.close()
      record
    }
  }

  implicit val javaSerializationPodRecord: SerializationFormat[PodRecord] = javaSerializationObjectFormat[PodRecord]
  implicit val javaSerializationReservationRecord: SerializationFormat[ReservationRecord] = javaSerializationObjectFormat[ReservationRecord]
}



trait SerializationFormat[T] {
  def writes(o: T): Array[Byte]
  def reads(blob: Array[Byte]): T
}