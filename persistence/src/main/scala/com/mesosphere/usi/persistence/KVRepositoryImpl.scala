package com.mesosphere.usi.persistence

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.{PodRecord, ReservationRecord}
import com.mesosphere.usi.persistence.storage.KVStorage
import com.mesosphere.usi.persistence.serialization.SerializationFormats.{javaSerializationPodRecord, javaSerializationReservationRecord}
import com.mesosphere.usi.persistence.serialization.Serialization

import scala.concurrent.Future

/**
  * Implementation of the repository that uses an underlying key-value storage for storing records.
  * @param storage
  */
class KVRepositoryImpl(storage: KVStorage) extends Repository {

  private def podRecordSeqNr2Bytes(seqNr: Long): Array[Byte] = {
    s"podRecord-$seqNr".getBytes()
  }

  private def reservatonRecordSeqNr2Bytes(seqNr: Long): Array[Byte] = {
    s"reservationRecord-$seqNr".getBytes()
  }

  override def storePodRecord(record: PodRecord): Future[Done] = {
    val value = Serialization.toBinary(record)
    val key = podRecordSeqNr2Bytes(record.sequenceNr)
    storage.write(key, value)
  }

  override def storeReservationRecord(record: ReservationRecord): Future[Done] = {
    val value = Serialization.toBinary(record)
    val key = reservatonRecordSeqNr2Bytes(record.sequenceNr)
    storage.write(key, value)
  }

  override def podRecords(fromSequenceNr: Long, toSequenceNr: Long): Source[PodRecord, NotUsed] = {
    Source.unfoldResourceAsync[Array[Byte], Iterator[Long]](() => {
      Future.successful((fromSequenceNr to Math.min(toSequenceNr, Int.MaxValue - 1)).iterator)
    }, it => {
      val seqNr = it.next()
      val key = podRecordSeqNr2Bytes(seqNr)
      storage.read(key)
    }, foo => {
      Future.successful(Done)
    })
      .map(bytes => Serialization.fromBinary[PodRecord](bytes))
  }

  override def reservationRecords(fromSequenceNr: Long, toSequenceNr: Long): Source[ReservationRecord, NotUsed] = {
    Source.unfoldResourceAsync[Array[Byte], Iterator[Long]](() => {
      Future.successful((fromSequenceNr to Math.min(toSequenceNr, Int.MaxValue - 1)).iterator)
    }, it => {
      val seqNr = it.next()
      val key = reservatonRecordSeqNr2Bytes(seqNr)
      storage.read(key)
    }, foo => {
      Future.successful(Done)
    })
      .map(bytes => Serialization.fromBinary[ReservationRecord](bytes))
  }
}
