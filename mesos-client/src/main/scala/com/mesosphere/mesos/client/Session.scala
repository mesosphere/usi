package com.mesosphere.mesos.client
import java.net.URL

import akka.{NotUsed, util}
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * A session captures the connection information with Mesos. This is among other things the URL and credentials providers.
  *
  * It behaves similar to Python Request's [[https://2.python-requests.org/en/master/user/advanced/#session-objects Session object]].
  * Thus it provides methods to construct and connect to Mesos.
  *
  * @param url The Mesos master URL.
  * @param streamId The Mesos stream ID. See the [[http://mesos.apache.org/documentation/latest/scheduler-http-api/#calls docs]] for details.
  * @param authorization A [[CredentialsProvider]] if the connection is secured.
  */
case class Session(url: URL, streamId: String, authorization: Option[CredentialsProvider] = None) {
  lazy val isSecured: Boolean = url.getProtocol == "https"
  lazy val port = if (url.getPort == -1) url.getDefaultPort else url.getPort

  /**
    * Construct a new [[HttpRequest]] for a serialized Mesos call and a set of authorization, ie session token.
    * @param bytes The bytes of the serialized Mesos call.
    * @param maybeCredentials The session token if required.
    * @return The [[HttpRequest]] with proper headers and body.
    */
  def createPostRequest(bytes: Array[Byte], maybeCredentials: Option[HttpCredentials]): HttpRequest = {
    println(s"Body: ${bytes.map(_.toChar).mkString}")
    HttpRequest(
      HttpMethods.POST,
      uri = Uri(s"$url/api/v1/scheduler"),
      entity = HttpEntity(MesosClient.ProtobufMediaType, bytes),
      headers = MesosClient.MesosStreamIdHeader(streamId) :: maybeCredentials.map(Authorization(_)).toList
    )
  }

  /** @return A flow that makes Mesos calls and outputs HTTP responses. */
  def post(implicit system: ActorSystem, mat: Materializer): Flow[Array[Byte], HttpResponse, NotUsed] =
    authorization match {
      case Some(credentialsProvider) =>
        implicit val askTimeout = util.Timeout(20.seconds)
        val sessionActor = system.actorOf(SessionActor.props(credentialsProvider, createPostRequest))
        Flow[Array[Byte]].ask[HttpResponse](1)(sessionActor)
      case None =>
        Flow[Array[Byte]].map(createPostRequest(_, None)).via(connection)
    }

  /**
    * A connection flow factory that will create a flow that only processes one request at a time.
    *
    * It will reconnect if the server closes the connection after a successful request.
    *
    * @return A connection flow for single requests.
    */
  private def connection(implicit system: ActorSystem, mat: Materializer): Flow[HttpRequest, HttpResponse, NotUsed] = {
    val poolSettings = ConnectionPoolSettings("").withMaxConnections(1).withPipeliningLimit(1)
    if (isSecured) {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().newHostConnectionPoolHttps(host = url.getHost, port = port, settings = poolSettings))
        .map {
          case (Success(response), NotUsed) => response
          case (Failure(ex), NotUsed) => throw ex
        }
    } else {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().newHostConnectionPool(host = url.getHost, port = port, settings = poolSettings))
        .map {
          case (Success(response), NotUsed) => response
          case (Failure(ex), NotUsed) => throw ex
        }
    }
  }
}

object SessionActor {
  def props(
      credentialsProvider: CredentialsProvider,
      requestFactory: (Array[Byte], Option[HttpCredentials]) => HttpRequest): Props = {
    Props(new SessionActor(credentialsProvider, requestFactory))
  }


  /**
    * A simple class that captures the original sender of a call and the serialized call.
    *
    * @param originalCall The serialized Mesos call.
    * @param originalSender The sender from the stream.
    * @param response The response for the call.
    */
  private [client] case class Response(originalCall: Array[Byte], originalSender: ActorRef, response: HttpResponse)
}

/**
  * An actor makes requests to Mesos using credentials from a [[CredentialsProvider]].
  *
  * @param credentialsProvider The provider used to fetch the credentials, ie a session token.
  * @param requestFactory The factory method that creates a [[HttpRequest]] from a serialized Mesos call. It should
  *                       capture the [[Session.streamId]] and [[Session.url]].
  */
class SessionActor(
    credentialsProvider: CredentialsProvider,
    requestFactory: (Array[Byte], Option[HttpCredentials]) => HttpRequest)
    extends Actor
    with Stash
    with StrictLogging {

  import context.dispatcher

  import akka.pattern.pipe

  override def preStart(): Unit = {
    super.preStart()
    credentialsProvider.nextToken().pipeTo(self)
  }

  override def receive: Receive = initializing

  def initializing: Receive = {
    case credentials: HttpCredentials =>
      logger.info("Retrieved token")
      context.become(initialized(credentials))
      unstashAll()
    case Status.Failure(ex) =>
      logger.error("Fetching the next token failed", ex)
      throw ex
    case _ => stash()
  }

  def initialized(credentials: HttpCredentials): Receive = {
    case call: Array[Byte] =>
      val request = requestFactory(call, Some(credentials))
      val originalSender = sender()
      logger.info("Processing next call.")
      Http()(context.system)
        .singleRequest(request)
        .onComplete {
          case Success(response) =>
            logger.info(s"HTTP response: ${response.status}")
            self ! SessionActor.Response(call, originalSender, response)
          case Failure(ex) =>
            logger.error("HTTP request failed", ex)
            // Fail stream.
            originalSender ! Status.Failure(ex)
        }
    case SessionActor.Response(originalCall, originalSender, response) =>
      logger.info(s"Call replied with ${response.status}")
      if (response.status == StatusCodes.Unauthorized) {
        logger.info("Refreshing token")

        context.become(initializing)
        credentialsProvider.nextToken().pipeTo(self)

        // Queue current call again.
        self ! originalCall
      } else {
        originalSender ! response
      }
  }
}
