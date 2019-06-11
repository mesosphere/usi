package com.mesosphere.mesos.client
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.HttpCredentials
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.Future

class SessionActorTest extends AkkaUnitTest {

  "The Session Actor" when {
//    "the token fetching fails" should {
//      "crash" in {
//        val f = new Fixture
//        EventFilter[UnsupportedOperationException](occurrences = 83).intercept {
//          system.actorOf(SessionActor.props(f.failingProvider, f.requestFactory))
//        }
//      }
//    }
  }

  class Fixture() {

    val failingProvider = new CredentialsProvider {
      override def nextToken() =
        Future.failed(new UnsupportedOperationException("This credentials provider will always fail."))
    }

    def requestFactory(body: Array[Byte], credentials: Option[HttpCredentials]): HttpRequest = ???
  }
}
