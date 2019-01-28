package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}

import scala.concurrent.Future

trait PodRecordRepository {

  def store(record: PodRecord): Future[Unit]

  def find(podId: PodId): Future[Option[PodRecord]]

}
