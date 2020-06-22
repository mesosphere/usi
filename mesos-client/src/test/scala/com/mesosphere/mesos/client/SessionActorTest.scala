package com.mesosphere.mesos.client

import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.{Future, Promise}

class SessionActorTest extends AkkaUnitTest {

  "The Session Actor" should {
    "replied with a response to the original sender" in {
      Given("A SessionActor and an HTTP response")
      val sessionActor =
        system.spawn(
          SessionActor(
            Some(BasicAuthenticationProvider("user", "password")),
            "some-strean-id",
            Uri("http://example.com")
          ),
          "SessionActor-reply"
        )
      val responsePromise = Promise[HttpResponse]()
      val httpResponse = HttpResponse(entity = HttpEntity("hello"))

      When("the actor receives a response for the request")
      sessionActor ! SessionActor.Response(SessionActor.Call(Array.emptyByteArray, responsePromise), httpResponse)

      Then("it sends the response to the original sender")
      responsePromise.future.futureValue shouldBe httpResponse
    }

    "make a simple call to Mesos" in withMesosStub(StatusCodes.OK) { mesos =>
      Given("A simple Mesos API stub and a SessionActor instance")
      val credentialsProvider = new CountingCredentialsProvider()
      val sessionActor =
        system.spawn(SessionActor(Some(credentialsProvider), "some-stream-id", mesos.uri), "SessionActor-simple-call")

      When("we make a call through the session actor")
      val responsePromise = Promise[HttpResponse]()
      sessionActor ! SessionActor.Call(Array.empty[Byte], responsePromise)
      val response = responsePromise.future.futureValue

      Then("we receive an HTTP 200")
      response.status should be(StatusCodes.OK)

      And("Mesos was called only once")
      Unmarshal(response.entity).to[String].futureValue.toInt should be(1)

      And("the session token was fetched only once")
      credentialsProvider.calls should be(1)
    }

    "refresh the session token when the first call response is an HTTP 401" in withMesosStub(
      StatusCodes.Unauthorized,
      StatusCodes.OK
    ) { mesos =>
      Given("A SessionActor instance")
      val credentialsProvider = new CountingCredentialsProvider()
      val sessionActor =
        system.spawn(SessionActor(Some(credentialsProvider), "some-stream-id", mesos.uri), "SessionActor-refresh")

      When("we make a call through the session actor")
      val responsePromise = Promise[HttpResponse]()
      sessionActor ! SessionActor.Call(Array.empty[Byte], responsePromise)
      val response = responsePromise.future.futureValue

      Then("we receive an HTTP 200")
      response.status should be(StatusCodes.OK)

      And("Mesos was called twice: one declined call and one with the fresh token")
      Unmarshal(response.entity).to[String].futureValue.toInt should be(2)

      And("the session token was fetched twice")
      credentialsProvider.calls should be(2)
    }

    "follow a redirect" in withMesosStub(StatusCodes.OK) { mesos =>
      Given("A SessionActor instance")
      val sessionActor =
        system.spawn(
          SessionActor(None, "some-stream-id", mesos.uri.withPath(Path("/redirected"))),
          "SessionActor-redirect"
        )

      When("we make a call through the session actor")
      val responsePromise = Promise[HttpResponse]()
      sessionActor ! SessionActor.Call(Array.empty[Byte], responsePromise)
      val response = responsePromise.future.futureValue

      Then("we receive an HTTP 200")
      response.status should be(StatusCodes.OK)

      And("/mesos was called once: the redirected call")
      Unmarshal(response.entity).to[String].futureValue.toInt should be(1)
    }
  }

  /**
    * A simple [[CredentialsProvider]] stub that counts how many times [[CredentialsProvider.nextToken()]] was called.
    */
  class CountingCredentialsProvider() extends CredentialsProvider {
    @volatile var calls: Int = 0

    private val credentials = BasicHttpCredentials("user", "password")

    override def nextToken(): Future[HttpCredentials] = {
      calls += 1
      Future.successful(credentials)
    }
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
  case class MesosStub(@volatile var responseCodes: List[StatusCode]) {
    import akka.http.scaladsl.server.Directives._

    @volatile var calls: Int = 0

    val route =
      post {
        concat(
          path("mesos") {
            calls += 1
            responseCodes match {
              case head :: tail =>
                responseCodes = tail
                complete(head -> calls.toString)
              case Nil =>
                complete(StatusCodes.NotImplemented -> "Provided response codes exhausted.")
            }
          },
          path("redirected") {
            redirect("/mesos", StatusCodes.TemporaryRedirect)
          }
        )
      }

    val binding = Http().bindAndHandle(route, "localhost", 0).futureValue
    val uri = Uri(s"http://localhost:${binding.localAddress.getPort}/mesos")
  }
}
