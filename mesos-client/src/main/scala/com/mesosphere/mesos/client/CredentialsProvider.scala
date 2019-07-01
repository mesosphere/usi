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
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import pdi.jwt.JwtJson
import pdi.jwt.JwtAlgorithm.RS256
import play.api.libs.json._
import play.api.libs.json.Reads._

import scala.async.Async.{async, await}
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
  * Provides DC/OS session tokens as JWT for service accounts.
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
case class DcosServiceAccountProvider(uid: String, privateKey: String, root: URL)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    context: ExecutionContext)
    extends CredentialsProvider
    with PlayJsonSupport
    with StrictLogging {

  private def expireIn(duration: Duration): Long = Instant.now.getEpochSecond + duration.toSeconds

  /**
    * Construct a claim to authenticate and retrieve a session token. The timeout is for this token ''not'' the session
    * token that we are requesting. That means if ou login request takes more than [[DcosServiceAccountProvider.SERVICE_LOGIN_TOKEN_LIFETIME]]
    * seconds the authentication will fail.
    *
    * @return a claim for the user that expires in [[DcosServiceAccountProvider.SERVICE_LOGIN_TOKEN_LIFETIME]] seconds.
    */
  private def claim: JsObject = Json.obj("uid" -> uid, "exp" -> expireIn(DcosServiceAccountProvider.SERVICE_LOGIN_TOKEN_LIFETIME))

  case class AuthenticationToken(token: String)
  implicit val authenticationTokenRead: Reads[AuthenticationToken] =
    (JsPath \ "token").read[String].map(AuthenticationToken)

  /** @return a new request for an authentication token. */
  def loginRequest: HttpRequest = {

    // Tokenize the claim.
    val serviceLoginToken = JwtJson.encode(claim, privateKey, RS256)
    val data: String = Json.stringify(Json.obj("uid" -> uid, "token" -> serviceLoginToken))

    HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"$root/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )
  }

  override def nextToken(): Future[HttpCredentials] = async {
    logger.info(s"Fetching next authentication token from $root")
    val response = await(Http().singleRequest(loginRequest))
    val AuthenticationToken(acsToken) = await(Unmarshal(response.entity).to[AuthenticationToken])
    GenericHttpCredentials("", Map("token" -> acsToken))
  }
}

object DcosServiceAccountProvider {
  val SERVICE_LOGIN_TOKEN_LIFETIME = 20.seconds
}

case class BasicAuthenticationProvider(user: String, password: String) extends CredentialsProvider {

  val credentials = BasicHttpCredentials(user, password)

  def nextToken(): Future[HttpCredentials] = Future.successful(credentials)
}
