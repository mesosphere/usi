package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future

/**
  * A simple in memory implementation of the [[PodRecordRepository]]. It should not be used in production but merely
  * defines a common behavior to all CRUD repositories used by USI.
  */
case class InMemoryPodRecordRepository() extends PodRecordRepository with StrictLogging {
  val data = mutable.Map.empty[PodId, PodRecord]

  override def create(record: PodRecord): Future[PodId] = synchronized {
    if (data.contains(record.podId)) {
      logger.warn(s"${record.podId.value} already exists.")
      Future.failed(RecordAlreadyExistsException(record.podId.value))
    } else {
      logger.info("Create record.")
      data += record.podId -> record
      Future.successful(record.podId)
    }
  }

  override def delete(podId: PodId): Future[PodId] = synchronized {
    if (!data.contains(podId)) {
      Future.failed(RecordNotFoundException(podId.value))
    } else {
      data -= podId
      Future.successful(podId)
    }
  }

  override def read(recordId: PodId): Future[Option[PodRecord]] = synchronized {
    logger.info(s"Read record from $data.")
    Future.successful(data.get(recordId))
  }

  override def update(record: PodRecord): Future[PodId] = synchronized {
    if (!data.contains(record.podId)) {
      Future.failed(RecordNotFoundException(record.podId.value))
    } else {
      data += record.podId -> record
      Future.successful(record.podId)
    }
  }
}
