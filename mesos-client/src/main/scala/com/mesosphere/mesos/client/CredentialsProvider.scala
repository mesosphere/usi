package com.mesosphere.mesos.client

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, GenericHttpCredentials, HttpCredentials}
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

case class JwtProvider(uid: String, privateKey: String, root: String)(implicit system: ActorSystem, materializer: ActorMaterializer, context: ExecutionContext) extends CredentialsProvider {
  import org.json4s._
  import org.json4s.JsonDSL._
  import JsonMethods.{parse, render, compact}

  def expireIn(duration: Duration): Long = Instant.now.getEpochSecond + duration.toSeconds

  def claim() = JObject(("uid", uid), ("exp", expireIn(60.minutes)))

  // TODO: Should probably be a flow at some point
  def getSessionToken()(
      implicit system: ActorSystem,
      context: ExecutionContext,
      materializer: ActorMaterializer): Future[String] = {
    val token = JwtJson4s.encode(claim(), privateKey, RS256)
    val data: String = compact(render(JObject(("uid", uid), ("token", token))))

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"$root/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )

    Http().singleRequest(request).flatMap { response =>
      // TODO: Use json unmarshaller directly.
      Unmarshal(response.entity).to[String].map { body =>
        println(s"Received: $body")
        (parse(body) \ "token") match {
          case JString(t) => t
          case _ => throw new IllegalArgumentException(s"Token is not a string in $body.")
        }
      }
    }
  }

  val token: String = {
    val tmp = Await.result(getSessionToken(), 5.minutes)
    println(s"Using token:\n$tmp")
    tmp
  }

  override def credentials(): HttpCredentials = GenericHttpCredentials("", Map("token" -> token))
}

case class BasicAuthenticationProvider(user: String, password: String) extends CredentialsProvider {

  override def credentials(): HttpCredentials = BasicHttpCredentials(user, password)
}
