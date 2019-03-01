package com.mesosphere.usi.repository

import com.mesosphere.utils.UnitTest
import com.typesafe.scalalogging.StrictLogging

class InMemoryRepositoryTest extends UnitTest with RepositoryBehavior with StrictLogging {

  "The in-memory pod record repository" should {
    behave like podRecordCreate(() => InMemoryPodRecordRepository.apply())
    behave like podRecordRead(() => InMemoryPodRecordRepository.apply())
    behave like podRecordDelete(() => InMemoryPodRecordRepository.apply())
    behave like podRecordUpdate(() => InMemoryPodRecordRepository.apply())
  }
}
