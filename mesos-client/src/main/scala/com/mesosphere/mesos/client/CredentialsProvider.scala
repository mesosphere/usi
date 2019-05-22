package com.mesosphere.mesos.client

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import org.json4s.native.JsonMethods
import pdi.jwt.JwtJson4s
import pdi.jwt.JwtAlgorithm.RS256

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

trait CredentialsProvider {

  def credentials(): HttpCredentials
  def header(): headers.Authorization = headers.Authorization(credentials())
  def refresh(): Unit = ???
}

case class JwtProvider(privateKey: String, root: String) extends CredentialsProvider {
  import org.json4s._
  import org.json4s.JsonDSL._
  import JsonMethods.{parse, render, compact}

  def expirationIn(duration: Duration): Long = Instant.now.getEpochSecond + duration.toSeconds

  def claim() = JObject(("uid", 1), ("exp", expirationIn(5.minutes)))

  // Should probably be a flow at some point
  def getToken()(
      implicit system: ActorSystem,
      context: ExecutionContext,
      materializer: ActorMaterializer): Future[String] = {
    val token = JwtJson4s.encode(claim(), privateKey, RS256)
    val data: String = compact(render(JObject(("uid", 1), ("token", token))))

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"$root/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )

    Http().singleRequest(request).flatMap { response =>
      // TODO: Use json unmarshaller directly.
      Unmarshal(response.entity).to[String].map(body => compact(render(parse(body) \ "token")))
    }
  }

  override def credentials(): HttpCredentials = ???
}

object JwtProvider {
  def main(args: Array[String]): Unit = {
    import ExecutionContext.Implicits.global

    implicit val system = ActorSystem("test")
    implicit val materializer = ActorMaterializer()

    Await.result(JwtProvider("some key", "http://example.com").getToken(), 10.minutes)
  }
}

case class BasicAuthenticationProvider(user: String, password: String) extends CredentialsProvider {

  override def credentials(): HttpCredentials = BasicHttpCredentials(user, password)
}
