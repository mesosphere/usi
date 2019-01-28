package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{ReservationId, ReservationRecord}

import scala.concurrent.Future

trait ReservationRecordRepository {

  def store(record: ReservationRecord): Future[Unit]

  def find(podId: ReservationId): Future[Option[ReservationRecord]]

}
