package com.mesosphere.mesos.client

import akka.pattern.ask
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.testkit.TestProbe
import akka.util.Timeout
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.duration._

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

    "make a simple call to Mesos" in withMesosStub(StatusCodes.OK) { mesos =>
      implicit val timeout = Timeout(2.seconds)

      Given("A simple Mesos API stub and a SessionActor instance")
      val sessionActor =
        system.actorOf(SessionActor.props(BasicAuthenticationProvider("user", "password"), mesos.createRequest))

      When("we make a call through the session actor")
      val response = (sessionActor ? Array.empty[Byte]).futureValue.asInstanceOf[HttpResponse]

      Then("we receive an HTTP 200")
      response.status should be(StatusCodes.OK)

      And("Mesos was called only once")
      Unmarshal(response.entity).to[String].futureValue.toInt should be(1)
    }

    "refresh the session token when the first call response is an HTTP 401" in withMesosStub(StatusCodes.Unauthorized, StatusCodes.OK) { mesos =>
      implicit val timeout = Timeout(2.seconds)

      Given("A SessionActor instance")
      val sessionActor =
        system.actorOf(SessionActor.props(BasicAuthenticationProvider("user", "password"), mesos.createRequest))

      When("we make a call through the session actor")
      val response = (sessionActor ? Array.empty[Byte]).futureValue.asInstanceOf[HttpResponse]

      Then("we receive an HTTP 200")
      response.status should be(StatusCodes.OK)

      And("Mesos was called only once")
      Unmarshal(response.entity).to[String].futureValue.toInt should be(2)
    }
  }

  class Fixture() {

    def requestFactory(body: Array[Byte], credentials: Option[HttpCredentials]): HttpRequest = ???

  }

  /**
    * Provides a [[MesosStub]] fixture and will make sure that the stub unbinds after the test run.
    *
    * @param responseCodes The HTTP codes used by the stub.
    * @param testCode The actual test definition.
    */
  def withMesosStub(responseCodes: StatusCode*)(testCode: MesosStub => Any): Unit = {
    val mesos = MesosStub(responseCodes.toList)
    try {
      testCode(mesos)
    } finally {
      mesos.binding.unbind().futureValue
    }
  }

  /**
    * A stub for the Mesos V1 call API.
    *
    * All request respond with the number of calls the endpoint received. Each response takes the head of [[responseCodes]].
    * If the list is exhausted the stub will respond with an HTTP 501 error.
    *
    * @param responseCodes The HTTP codes the stub will use for [[responseCodes.length]] requests.
    */
  case class MesosStub(var responseCodes: List[StatusCode]) {
    import akka.http.scaladsl.server.Directives._

    var calls: Int = 0

    val route =
      path("mesos") {
        post {
          calls += 1
          responseCodes match {
            case head :: tail =>
              responseCodes = tail
              complete(head -> calls.toString)
            case Nil =>
              complete(StatusCodes.NotImplemented -> "Provided response codes exhausted.")
          }
        }
      }

    val binding = Http().bindAndHandle(route, "localhost", 0).futureValue
    val uri = Uri(s"http://localhost:${binding.localAddress.getPort}/mesos")

    def createRequest(body: Array[Byte], credentials: Option[HttpCredentials]): HttpRequest = HttpRequest(HttpMethods.POST, uri = uri)
  }
}
