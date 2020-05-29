package com.mesosphere.mesos.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.model._
import akka.stream.WatchedActorTerminatedException
import akka.stream.scaladsl.{Flow, FlowWithContext}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * A session captures the connection information with Mesos. This is among other things the URL and credentials providers.
  *
  * It behaves similar to Python Request's [[https://2.python-requests.org/en/master/user/advanced/#session-objects Session object]].
  * Thus it provides methods to construct and connect to Mesos.
  *
  * @param baseUri The Mesos master URL.
  * @param streamId The Mesos stream ID. See the [[http://mesos.apache.org/documentation/latest/scheduler-http-api/#calls docs]] for details.
  * @param authorization A [[CredentialsProvider]] if the connection is secured.
  */
case class Session(baseUri: Uri, streamId: String, authorization: Option[CredentialsProvider] = None)
    extends StrictLogging {

  /**
    * This is a port of [[Flow.ask()]] that supports a tuple to carry a context `C`.
    */
  private def sessionActorFlow[C](parallelism: Int)(ref: ActorRef[SessionActor.Message])(
      implicit ec: ExecutionContext): Flow[(Array[Byte], C), (HttpResponse, C), NotUsed] = {
    Flow[(Array[Byte], C)]
      .watch(ref.toClassic)
      .mapAsync(parallelism) {
        case (el, ctx) => SessionActor.call(ref, el).map(o => (o, ctx))
      }
      .mapError {
        // the purpose of this recovery is to change the name of the stage in that exception
        // we do so in order to help users find which stage caused the failure -- "the ask stage"
        case ex: WatchedActorTerminatedException =>
          throw new WatchedActorTerminatedException("ask()", ex.ref)
      }
      .named("ask")
  }

  /** @return A flow that makes Mesos calls and outputs HTTP responses. */
  def post[C](implicit system: ActorSystem): FlowWithContext[Array[Byte], C, HttpResponse, C, NotUsed] = {
    import system.dispatcher
    logger.info(s"Create authenticated session for stream $streamId.")
    val sessionActor =
      system.spawn(
        SessionActor(authorization, streamId, baseUri.withPath(Path("/api/v1/scheduler"))),
        s"SessionActor-$streamId")

    FlowWithContext[Array[Byte], C].via(sessionActorFlow(1)(sessionActor))
  }
}

object Session {

  /** @return whether the url defines a secured connection. */
  def isSecured(uri: Uri): Boolean = {
    uri.scheme match {
      case "https" => true
      case "http" => false
      case other =>
        throw new IllegalArgumentException(s"'$other' is not a supported protocol. Only HTTPS and HTTP are supported.")
    }
  }
}

object SessionActor {

  def apply(authorization: Option[CredentialsProvider], streamId: String, schedulerEndpoint: Uri): Behavior[Message] = {
    // TODO: configure stash size
    Behaviors.withStash(100) { buffer =>
      Behaviors.setup { context =>
        new SessionActor(authorization, streamId, schedulerEndpoint, context, buffer).start()
      }
    }
  }

  sealed trait Message

  /**
    * A Message containing the HTTP credentials required to make calls to Mesos.
    *
    * @param value The credentials. See [[CredentialsProvider.nextToken()]] for details.
    */
  private[client] case class Credentials(value: HttpCredentials) extends Message

  /**
    * A Message sent to the actor when the credentials could not be loaded.
    *
    * @param cause The exception returned by [[CredentialsProvider.nextToken()]].
    */
  private[client] case class CredentialsError(cause: Throwable) extends Message

  /**
    * Calls the session actor with the body of the request.
    *
    * @param ref A reference to the [[SessionActor]].
    * @param body The body of the call.
    * @return A future [[HttpResponse]] of the call.
    */
  private[client] def call(ref: ActorRef[SessionActor.Message], body: Array[Byte]): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]()
    ref ! SessionActor.Call(body, promise)
    promise.future
  }

  /**
    * Captures the serialized call and promise of the call
    *
    * @param body The serialized Mesos call
    * @param responsePromise The promise that is fulfilled with the response of the call
    */
  private[client] case class Call(body: Array[Byte], responsePromise: Promise[HttpResponse]) extends Message

  /**
    * Captures the response, the promise of a call and the serialized call.
    *
    * @param originalCall The original call that resulted in the response.
    * @param response The response for the call.
    */
  private[client] case class Response(originalCall: Call, response: HttpResponse) extends Message
}

/**
  * An actor makes requests to Mesos using credentials from a [[CredentialsProvider]].
  *
  * @param authorization The provider used to fetch the credentials, ie a session token, or None for unauthenticated
  *                       calls
  * @param streamId The Mesos stream ID. See the [[http://mesos.apache.org/documentation/latest/scheduler-http-api/#calls docs]] for details.
  * @param schedulerEndpoint The Mesos master URL /api/v1/scheduler.
  */
class SessionActor(
    authorization: Option[CredentialsProvider],
    streamId: String,
    schedulerEndpoint: Uri,
    context: ActorContext[SessionActor.Message],
    buffer: StashBuffer[SessionActor.Message])
    extends StrictLogging {

  import SessionActor._

  // TODO:Should we still use this dispatcher?
  implicit val dispatcher = context.system.dispatchers.lookup(DispatcherSelector.default())

  def start(): Behavior[Message] = {
    authorization match {
      case Some(credentialsProvider) =>
        fetchNextToken(credentialsProvider)
        initializing
      case None => initialized(schedulerEndpoint, None)
    }
  }

  /**
    * Initial behavior that fetched the initial auth token.
    */
  def initializing: Behavior[Message] = {
    Behaviors.receiveMessage {
      case Credentials(credentials) =>
        logger.debug("Retrieved IAM authentication token")
        buffer.unstashAll(initialized(schedulerEndpoint, Some(credentials)))
      case CredentialsError(cause) =>
        logger.error("Fetching the next IAM authentication token failed", cause)
        throw cause
      case other =>
        buffer.stash(other)
        Behaviors.same
    }
  }

  /**
    * Default behaviour when the session has an auth token.
    *
    * @param requestUri The endpoint for the requests. Should be <Mesos leader>/api/v1/scheduler.
    * @param maybeCredentials The auth token if the requests need to be authenticated.
    * @return A behavior that can process Mesos calls.
    */
  def initialized(requestUri: Uri, maybeCredentials: Option[HttpCredentials]): Behavior[Message] = {
    Behaviors.receiveMessagePartial {
      case call: Call =>
        val request = createPostRequest(call.body, requestUri, maybeCredentials)
        logger.debug(s"Processing next Mesos call: $request")
        // The TLS handshake for each connection might be an overhead. We could potentially reuse a connection.
        Http()(context.system.classicSystem)
          .singleRequest(request)
          .onComplete {
            case Success(response) =>
              logger.debug(s"Mesos call HTTP response: ${response.status}")
              context.self ! SessionActor.Response(call, response)
            case f @ Failure(ex) =>
              logger.error("Mesos call HTTP request failed", ex)
              // Fail stream.
              call.responsePromise.complete(f)
          }
        Behaviors.same
      case SessionActor.Response(originalCall, response) =>
        logger.debug(s"Call replied with ${response.status}")
        if (response.status == StatusCodes.Unauthorized) {
          logger.info("Refreshing IAM authentication token")

          authorization match {
            case Some(credentialsProvider) => fetchNextToken(credentialsProvider)
            case None =>
              throw new IllegalStateException("Received HTTP 401 Unauthorized but no credential provider was supplied.")
          }

          // Queue current call again. It will be unstashed/sent once the actor becomes initialized again.
          buffer.stash(originalCall)

          initializing
        } else if (response.status.isRedirection()) {
          logger.debug(s"Received redirect $response")

          // Reset scheduler endpoint.
          val locationHeader = response.header[headers.Location].get
          val redirectUri = locationHeader.uri.resolvedAgainst(requestUri)

          // Queue current call again.
          buffer.stash(originalCall)

          // Change redirect URI and unstash/sent the call again.
          buffer.unstash(initialized(redirectUri, maybeCredentials), 1, identity)

        } else {
          logger.debug("Responding to original sender")
          originalCall.responsePromise.success(response)
          Behaviors.same
        }
    }
  }

  /**
    * Construct a new [[HttpRequest]] for a serialized Mesos call and a set of authorization, ie session token.
    * @param bytes The bytes of the serialized Mesos call.
    * @param endpoint The endpoint for the request.
    * @param maybeCredentials The session token if required.
    * @return The [[HttpRequest]] with proper headers and body.
    */
  private def createPostRequest(
      bytes: Array[Byte],
      endpoint: Uri,
      maybeCredentials: Option[HttpCredentials]): HttpRequest = {
    HttpRequest(
      HttpMethods.POST,
      uri = endpoint,
      entity = HttpEntity(MesosClient.ProtobufMediaType, bytes),
      headers = MesosClient.MesosStreamIdHeader(streamId) :: maybeCredentials.map(Authorization(_)).toList
    )
  }

  /**
    * Fetches the next token and send it packaged in a message to the actor again.
    */
  private def fetchNextToken(provider: CredentialsProvider): Unit = {
    context.pipeToSelf(provider.nextToken()) {
      case Success(credentials) => Credentials(credentials)
      case Failure(ex) => CredentialsError(ex)
    }
  }
}
