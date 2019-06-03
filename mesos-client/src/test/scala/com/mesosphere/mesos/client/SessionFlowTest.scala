package com.mesosphere.mesos.client
import akka.NotUsed
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import akka.stream.Supervision
import akka.stream.scaladsl.{Flow, RestartFlow, Source}
import com.mesosphere.utils.AkkaUnitTest

import scala.concurrent.Future
import scala.concurrent.duration._

class SessionFlowTest extends AkkaUnitTest {

  "The Session flow" should {
    "restart" ignore {
      val f = new Fixture()
      val calls = List(
        "first".toCharArray.map(_.toByte),
        "second".toCharArray.map(_.toByte),
        "first".toCharArray.map(_.toByte),
        "second".toCharArray.map(_.toByte))
      val flow: Flow[Array[Byte], HttpResponse, NotUsed] =
        Flow.fromGraph(SessionFlow(f.credentialsProvider, f.createRequest)).zipWithIndex.map {
          case (request, index) =>
            logger.info(s"Processing $index")
            if (index % 2 == 0) throw new IllegalStateException(s"Failed on $index")
            HttpResponse()
        }
      Source(calls)
        .via(RestartFlow.withBackoff(1.second, 1.second, 1.0, 3)(() => flow))
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

    def createRequest(body: Array[Byte], creds: Option[HttpCredentials]) = HttpRequest()

    val decider: Supervision.Decider = {
      case _: IllegalStateException => Supervision.Restart
      case _ => Supervision.Stop
    }

  }
}
