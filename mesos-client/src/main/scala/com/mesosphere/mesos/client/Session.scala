package com.mesosphere.mesos.client
import java.net.URL

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, FlowWithContext}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.scheduler.Protos.Call

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
case class Session(url: URL, streamId: String, authorization: Option[CredentialsProvider] = None)(
    implicit askTimout: Timeout)
    extends StrictLogging {

  /**
    * Construct a new [[HttpRequest]] for a serialized Mesos call and a set of authorization, ie session token.
    * @param bytes The bytes of the serialized Mesos call.
    * @param maybeCredentials The session token if required.
    * @return The [[HttpRequest]] with proper headers and body.
    */
  def createPostRequest(bytes: Array[Byte], maybeCredentials: Option[HttpCredentials]): HttpRequest = {
    HttpRequest(
      HttpMethods.POST,
      uri = Uri(s"$url/api/v1/scheduler"),
      entity = HttpEntity(MesosClient.ProtobufMediaType, bytes),
      headers = MesosClient.MesosStreamIdHeader(streamId) :: maybeCredentials.map(Authorization(_)).toList
    )
  }

  /** @return A flow that makes Mesos calls and outputs HTTP responses. */
  def post[C](
      implicit system: ActorSystem,
      mat: Materializer): FlowWithContext[Array[Byte], C, HttpResponse, C, NotUsed] =
    authorization match {
      case Some(credentialsProvider) =>
        logger.info(s"Create authenticated session for stream $streamId.")
        val sessionActor = system.actorOf(SessionActor.props(credentialsProvider, createPostRequest))
        // TODO: mapAsync is not handling all error cases such as ask.
        FlowWithContext[Array[Byte], C]
          .mapAsync(1) { el =>
            akka.pattern.ask(sessionActor).?(el)
          }
          .map(_.asInstanceOf[HttpResponse]) //.ask[HttpResponse](1)(sessionActor)
      case None =>
        logger.info(s"Create unauthenticated session for stream $streamId.")
        FlowWithContext[Array[Byte], C].map(createPostRequest(_, None)).via(connection)
    }

  /**
    * A connection flow factory that will create a flow that only processes one request at a time.
    *
    * It will reconnect if the server closes the connection after a successful request.
    *
    * @return A connection flow for single requests.
    */
  private def connection[C](
      implicit system: ActorSystem,
      mat: Materializer): FlowWithContext[HttpRequest, C, HttpResponse, C, NotUsed] = {
    // Constructs the connection pool settings with defaults and overrides the max connections and pipelining limit so
    // that only one request at a time is processed. See https://doc.akka.io/docs/akka-http/current/configuration.html
    // for details.
    // *IMPORTANT*: DO NOT CHANGE maxConnections OR pipelining Limit! Otherwise, USI won't guarantee request order to Mesos!
    val poolSettings = ConnectionPoolSettings("").withMaxConnections(1).withPipeliningLimit(1)

    val flow = Flow[(HttpRequest, C)]
      .via(if (Session.isSecured(url)) {
        Http()
          .newHostConnectionPoolHttps(host = url.getHost, port = Session.effectivePort(url), settings = poolSettings)
      } else {
        Http().newHostConnectionPool(host = url.getHost, port = Session.effectivePort(url), settings = poolSettings)
      })
      .map {
        case (Success(response), context) => response -> context
        case (Failure(ex), _) => throw ex
      }

    FlowWithContext.fromTuples(flow)
  }
}

object Session {

  /** @return whether the url defines a secured connection. */
  def isSecured(url: URL): Boolean = {
    url.getProtocol match {
      case "https" => true
      case "http" => false
      case other =>
        throw new IllegalArgumentException(s"$other is not a supported protocol. Only HTTPS and HTTP are supported.")
    }
  }

  /** @return The defined port or default port for given protocol. */
  def effectivePort(url: URL): Int = if (url.getPort == -1) url.getDefaultPort else url.getPort
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
  private[client] case class Response(originalCall: Array[Byte], originalSender: ActorRef, response: HttpResponse)
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
      logger.info("Retrieved IAM authentication token")
      context.become(initialized(credentials))
      unstashAll()
    case Status.Failure(ex) =>
      logger.error("Fetching the next IAM authentication token failed", ex)
      throw ex
    case _ => stash()
  }

  def initialized(credentials: HttpCredentials): Receive = {
    case call: Array[Byte] =>
      val request = requestFactory(call, Some(credentials))
      val originalSender = sender()
      logger.info("Processing next Mesos call.")
      // The TLS handshake for each connection might be an overhead. We could potentially reuse a connection.
      Http()(context.system)
        .singleRequest(request)
        .onComplete {
          case Success(response) =>
            logger.info(s"Mesos call HTTP response: ${response.status}")
            self ! SessionActor.Response(call, originalSender, response)
          case Failure(ex) =>
            logger.error("Mesos call HTTP request failed", ex)
            // Fail stream.
            originalSender ! Status.Failure(ex)
        }
    case SessionActor.Response(originalCall, originalSender, response) =>
      logger.debug(s"Call replied with ${response.status}")
      if (response.status == StatusCodes.Unauthorized) {
        logger.info("Refreshing IAM authentication token")

        context.become(initializing)
        credentialsProvider.nextToken().pipeTo(self)

        // Queue current call again.
        self.tell(originalCall, originalSender)
      } else {
        logger.info("Responding to original sender")
        originalSender ! response
      }
  }
}
