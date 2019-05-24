package com.mesosphere.mesos.client

import java.net.URL
import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, GenericHttpCredentials, HttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.json4s.native.JsonMethods
import pdi.jwt.JwtJson4s
import pdi.jwt.JwtAlgorithm.RS256

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait CredentialsProvider {

  def tokens(): Source[HttpCredentials, NotUsed]
}

case class JwtProvider(uid: String, privateKey: String, root: URL)(implicit system: ActorSystem, materializer: ActorMaterializer, context: ExecutionContext) extends CredentialsProvider {
  import org.json4s._
  import org.json4s.JsonDSL._
  import JsonMethods.{parse, render, compact}

  private def expireIn(duration: Duration): Long = Instant.now.getEpochSecond + duration.toSeconds
  private val claim = JObject(("uid", uid), ("exp", expireIn(60.minutes))) // TODO: configure token expiration.

  val acsTokenRequest: HttpRequest = {
    val token = JwtJson4s.encode(claim, privateKey, RS256)
    val data: String = compact(render(JObject(("uid", uid), ("token", token))))

    HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"${root.getPath}/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )
  }

  /**
    * Provides a [[Source]] of DC/OS acs tokens wrapped in [[HttpCredentials]]. The token is refreshed on each pull.
    *
    * @return A new source of [[HttpCredentials]]
    */
  def tokens(): Source[HttpCredentials, NotUsed] = {
    val port = if(root.getPort == -1) root.getDefaultPort else root.getPort

    Source.repeat(acsTokenRequest)
      .via(Http().outgoingConnectionHttps(root.getHost, port))
      .mapAsync(1) { response =>
        // TODO: Use json unmarshaller directly.
        Unmarshal(response.entity).to[String].map { body =>
          (parse(body) \ "token") match {
            case JString(acsToken) => GenericHttpCredentials("", Map("token" -> acsToken))
            case _ => throw new IllegalArgumentException(s"Token is not a string in $body.")
          }
        }
      }
  }
}

case class BasicAuthenticationProvider(user: String, password: String) extends CredentialsProvider {

  val credentials = BasicHttpCredentials(user, password)

  /**
    * Provides a [[Source]] of basic authentication credentials. They will never change, ie a second pull is basically a
    * no-op.
    *
    * @return A new source of [[HttpCredentials]]
    */
  def tokens(): Source[HttpCredentials, NotUsed] = Source.repeat(credentials)
}
