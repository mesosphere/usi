package com.mesosphere.usi.repository

import com.mesosphere.utils.UnitTest
import com.typesafe.scalalogging.StrictLogging

class InMemoryRepositoryTest extends UnitTest with RepositoryBehavior with StrictLogging {

  "The in-memory pod record repository" should {
    behave like podRecordStore(() => InMemoryPodRecordRepository.apply())
    behave like podRecordReadAll(() => InMemoryPodRecordRepository.apply())
    behave like podRecordDelete(() => InMemoryPodRecordRepository.apply())
  }
}
