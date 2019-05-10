package com.mesosphere.usi.repository

import com.mesosphere.utils.UnitTest
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.scalalogging.StrictLogging

class InMemoryRepositoryTest extends UnitTest with RepositoryBehavior with StrictLogging {

  "The in-memory pod record repository" should {
    behave like podRecordStore(() => InMemoryPodRecordRepository())
    behave like podRecordReadAll(() => InMemoryPodRecordRepository())
    behave like podRecordDelete(() => InMemoryPodRecordRepository())
  }
}
