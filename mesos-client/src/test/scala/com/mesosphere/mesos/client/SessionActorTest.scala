package com.mesosphere.mesos.client
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.testkit.TestProbe
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.Future

class SessionActorTest extends AkkaUnitTest {

  "The Session Actor" should {
    "replied with a response to the original sender" in {
      val f = new Fixture

      Given("A SessionActor and an HTTP response")
      val sessionActor =
        system.actorOf(SessionActor.props(BasicAuthenticationProvider("user", "password"), f.requestFactory))
      val originalSender = TestProbe()
      val httpResponse = HttpResponse(entity = HttpEntity("hello"))

      When("the actor receives a response for the request")
      sessionActor ! SessionActor.Response(Array.emptyByteArray, originalSender.ref, httpResponse)

      Then("it sends the response to the original sender")
      originalSender.expectMsg(httpResponse)
    }
  }

  class Fixture() {

    val failingProvider = new CredentialsProvider {
      override def nextToken() =
        Future.failed(new UnsupportedOperationException("This credentials provider will always fail."))
    }

    def requestFactory(body: Array[Byte], credentials: Option[HttpCredentials]): HttpRequest = ???
  }
}
