package com.mesosphere.usi.repository

import com.mesosphere.utils.UnitTest
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InMemoryRepositoryTest extends UnitTest with RepositoryBehavior with StrictLogging {

  "The in-memory pod record repository" should { behave like podRecordRepository(InMemoryPodRecordRepository.apply) }
}
