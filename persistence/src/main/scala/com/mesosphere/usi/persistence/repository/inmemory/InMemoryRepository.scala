package com.mesosphere.usi.persistence.repository.inmemory

import com.mesosphere.usi.core.models.{PodId, PodRecord, ReservationId, ReservationRecord}
import com.mesosphere.usi.persistence.repository.Repository

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

/**
  * In-memory implementation of the repository which doesn't use any persistent storage
  * and can be used for development/test purpose.
  */
class InMemoryRepository extends Repository {

  private val podMap = TrieMap.empty[PodId, PodRecord]
  private val reservationMap = TrieMap.empty[ReservationId, ReservationRecord]

  override def storePodRecord(record: PodRecord): Future[Unit] = {
    podMap.put(record.podId, record)
    Future.successful(Unit)
  }

  override def storeReservationRecord(record: ReservationRecord): Future[Unit] = {
    reservationMap.put(record.reservationId, record)
    Future.successful(Unit)
  }


  override def findPodRecord(id: PodId): Future[Option[PodRecord]] = {
    Future.successful(podMap.get(id))
  }


  override def findReservationRecord(id: ReservationId): Future[Option[ReservationRecord]] = {
    Future.successful(reservationMap.get(id))
  }
}
