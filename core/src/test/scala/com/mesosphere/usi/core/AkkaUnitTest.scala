package com.mesosphere.usi.core

import akka.actor.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}

trait AkkaUnitTest extends Suite with BeforeAndAfterAll with ScalaFutures {
  implicit val actorSystem = ActorSystem()

  override def afterAll(): Unit = {
    super.afterAll()
    actorSystem.terminate().futureValue
  }
}
