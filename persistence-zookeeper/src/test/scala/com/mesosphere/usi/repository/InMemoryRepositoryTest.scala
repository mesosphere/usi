package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.mesosphere.utils.UnitTest
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
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

  "The in-memory pod record repository" should { behave like podRecordRepository(InMemoryRepository()) }
}
