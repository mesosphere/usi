package com.mesosphere.usi.persistence

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.{PodRecord, ReservationRecord}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryRepositoryImpl extends Repository {

  private val podMap = TrieMap.empty[Long, PodRecord]
  private val reservationMap = TrieMap.empty[Long, ReservationRecord]

  override def storePodRecord(record: PodRecord): Future[Done] = {
    podMap.put(record.sequenceNr, record)
    Future.successful(Done)
  }

  override def storeReservationRecord(record: ReservationRecord): Future[Done] = {
    reservationMap.put(record.sequenceNr, record)
    Future.successful(Done)
  }

  override def podRecords(fromSequenceNr: Long, toSequenceNr: Long): Source[PodRecord, NotUsed] = {
    Source.unfoldResource[PodRecord, Iterator[Long]](() => {
      (fromSequenceNr to toSequenceNr).iterator
    }, it => {
      val seqNr = it.next()
      podMap.get(seqNr)
    }, it => ())
  }

  override def reservationRecords(fromSequenceNr: Long, toSequenceNr: Long): Source[ReservationRecord, NotUsed] = {
    Source.unfoldResource[ReservationRecord, Iterator[Long]](() => {
      (fromSequenceNr to toSequenceNr).iterator
    }, it => {
      val seqNr = it.next()
      reservationMap.get(seqNr)
    }, it => ())
  }
}
