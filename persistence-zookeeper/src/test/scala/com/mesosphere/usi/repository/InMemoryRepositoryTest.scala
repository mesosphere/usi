package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.mesosphere.utils.UnitTest
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future

class InMemoryRepositoryTest extends UnitTest with RepositoryBehavior with StrictLogging {

  case class InMemoryRepository() extends PodRecordRepository {
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

    override def delete(record: PodRecord): Future[PodId] = ???

    override def read(recordId: PodId): Future[Option[PodRecord]] = synchronized {
      logger.info(s"Read record from $data.")
      Future.successful(data.get(recordId))
    }

    override def update(record: PodRecord): Future[PodId] = ???
  }

  "InMemoryRepository" should { behave like podRecordRepository(InMemoryRepository()) }
}
