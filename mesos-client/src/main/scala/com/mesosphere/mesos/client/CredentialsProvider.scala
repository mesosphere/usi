package com.mesosphere.mesos.client

import java.net.URL
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, GenericHttpCredentials, HttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging
import org.json4s.native.JsonMethods
import pdi.jwt.JwtJson4s
import pdi.jwt.JwtAlgorithm.RS256

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Base credential provider for [[MesosClient]].
  */
trait CredentialsProvider {

  /** @return the next [[HttpCredentials]]. This might be a noop, eg for [[BasicAuthenticationProvider]]. */
  def nextToken(): Future[HttpCredentials]
}

/**
  * Provides DC/OS session tokens as JWT.
  *
  * @param uid The user id.
  * @param privateKey The JWT secret.
  * @param root The DC/OS root URL.
  * @param system The [[ActorSystem]] used for HTTP requests.
  * @param materializer The [[ActorMaterializer]] use to unmarshal HTTP responses.
  * @param context The execution context for transforming the future HTTP response.
  * @see See [[https://docs.mesosphere.com/1.11/security/ent/iam-api/#obtaining-an-authentication-token]] for details on
  *      the protocol.
  */
case class JwtProvider(uid: String, privateKey: String, root: URL)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    context: ExecutionContext)
    extends CredentialsProvider
    with StrictLogging {
  import org.json4s._
  import org.json4s.JsonDSL._
  import JsonMethods.{parse, render, compact}

  private def expireIn(duration: Duration): Long = Instant.now.getEpochSecond + duration.toSeconds
  private val claim = JObject("uid" -> uid, "exp" -> expireIn(20.seconds)) // TODO: configure token expiration.

  val acsTokenRequest: HttpRequest = {
    val token = JwtJson4s.encode(claim, privateKey, RS256)
    val data: String = compact(render(JObject(("uid", uid), ("token", token))))

    HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"$root/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )
  }

  override def nextToken(): Future[HttpCredentials] = {
    logger.debug(s"Fetching next token from $root")
    Http().singleRequest(acsTokenRequest).flatMap { response =>
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

  def nextToken(): Future[HttpCredentials] = Future.successful(credentials)
}
