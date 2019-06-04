package com.mesosphere.mesos.client
import java.net.URL

import akka.NotUsed
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.Future
import scala.util.{Success, Try}

class SessionFlowTest extends AkkaUnitTest {

  "The Session flow" should {
    "restart" in {
      val f = new Fixture()
      val calls = List(
        "first".toCharArray.map(_.toByte),
        "second".toCharArray.map(_.toByte),
        "third".toCharArray.map(_.toByte),
        "fourth".toCharArray.map(_.toByte),
        "fifth".toCharArray.map(_.toByte)
      )

      val connection: Flow[HttpRequest, Try[HttpResponse], NotUsed] =
        Flow[HttpRequest].zipWithIndex.map {
          case (request, index) =>
            logger.info(s"Processing $index")
            if (index % 2 == 1) Success(HttpResponse(StatusCodes.Unauthorized))
            else Success(HttpResponse())
        }
      Source(calls)
        .via(f.session.post(connection))
        .runForeach(println)
        .futureValue
    }
  }

  class Fixture {
    val credentialsProvider = new CredentialsProvider {
      override def nextToken(): Future[HttpCredentials] = {
        logger.info("Next token requested")
        Future.successful(BasicHttpCredentials("foo", "bar"))
      }
    }

    val session = Session(new URL("https://example.com"), "streamdid", Some(credentialsProvider))

  }
}
