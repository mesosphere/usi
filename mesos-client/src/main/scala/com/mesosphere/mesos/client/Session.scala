package com.mesosphere.mesos.client
import java.net.URL

import akka.{NotUsed, util}
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.scaladsl.{Flow, RestartFlow}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

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

  // Fail and trigger restart if the call was unauthorized.
  def handleRejection(response: Try[HttpResponse]): Try[HttpResponse] = {
    response match {
      case Success(r) if r.status == StatusCodes.Unauthorized =>
        throw new IllegalStateException("Session token expired.")
      case id => id
    }
  }

  /** @return A flow that transforms serialized Mesos calls to proper HTTP requests. */
  def post(connection: Flow[HttpRequest, Try[HttpResponse], NotUsed]): Flow[Array[Byte], Try[HttpResponse], NotUsed] =
    authorization match {
      case Some(credentialsProvider) =>
        RestartFlow.withBackoff(1.second, 1.second, 1, 3)(() =>
          Flow.fromGraph(SessionFlow(credentialsProvider, createPostRequest)).via(connection).map(handleRejection))
      case None =>
        Flow[Array[Byte]].map(createPostRequest(_, None)).via(connection)
    }

  def postSimple(implicit system: ActorSystem): Flow[Array[Byte], Try[HttpResponse], NotUsed] =
    authorization match {
      case Some(credentialsProvider) =>
        implicit val askTimeout = util.Timeout(20.seconds)
        val sessionActor = system.actorOf(SessionActor.props(credentialsProvider, createPostRequest))
        Flow[Array[Byte]].ask[HttpResponse](1)(sessionActor).map(Success(_))
      case None =>
        Flow[Array[Byte]].map(createPostRequest(_, None)).mapAsync(1)(Http().singleRequest(_)).map(Success(_))
    }

  /** @return The connection pool for this session. */
  def connectionPool(implicit system: ActorSystem): Flow[HttpRequest, Try[HttpResponse], NotUsed] = {
    // Disable pipelining.
    val poolSettings = ConnectionPoolSettings("").withMaxConnections(1).withPipeliningLimit(1)
    if (isSecured) {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().cachedHostConnectionPoolHttps(host = url.getHost, port = port, settings = poolSettings))
        .map(_._1)
    } else {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().cachedHostConnectionPool(host = url.getHost, port = port, settings = poolSettings))
        .map(_._1)
    }
  }
}

object SessionActor {
  def props(
      credentialsProvider: CredentialsProvider,
      requestFactory: (Array[Byte], Option[HttpCredentials]) => HttpRequest): Props = {
    Props(new SessionActor(credentialsProvider, requestFactory))
  }
}

class SessionActor(
    credentialsProvider: CredentialsProvider,
    requestFactory: (Array[Byte], Option[HttpCredentials]) => HttpRequest)
    extends Actor
    with Stash
    with StrictLogging {
  import akka.pattern.pipe
  import scala.concurrent.ExecutionContext.Implicits.global

  case class Response(originalCall: Array[Byte], originalSender: ActorRef, response: HttpResponse)

  override def preStart(): Unit = {
    super.preStart()
    credentialsProvider.nextToken().pipeTo(self)
  }

  override def receive: Receive = initializing

  def initializing: Receive = {
    // TODO: Handle nextToken errors.
    case credentials: HttpCredentials =>
      logger.info("Retrieved token")
      context.become(initialized(credentials))
      unstashAll()
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
            self ! Response(call, originalSender, response)
          case Failure(ex) =>
            logger.error("HTTP request failed", ex)
            // Fail stream.
            originalSender ! Status.Failure(ex)
        }
    case Response(originalCall, originalSender, response) =>
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

case class SessionFlow(
    credentialsProvider: CredentialsProvider,
    requestFactory: (Array[Byte], Option[HttpCredentials]) => HttpRequest)
    extends GraphStage[FlowShape[Array[Byte], HttpRequest]] {

  private val callsInlet = Inlet[Array[Byte]]("mesosCalls")
  private val requestsOutlet = Outlet[HttpRequest]("httpRequests")
  override val shape = FlowShape.of(callsInlet, requestsOutlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StrictLogging {

      var token = Option.empty[HttpCredentials]

      /** @return whether the session flow is initialized and has a sessions token set. */
      def isInitialized: Boolean = token.isDefined

      // Handle session token request and start pulling if downstream is ready.
      val startGraph = this.getAsyncCallback[Try[HttpCredentials]] {
        case Success(nextToken) =>
          logger.debug("Initialized session flow.")
          token = Some(nextToken)
          if (isAvailable(requestsOutlet)) pull(callsInlet)
        case Failure(ex) =>
          logger.error("Could not fetch session token", ex)
          this.failStage(ex)
      }

      /**
        * Initialize session flow by fetching the next session token. The session flow will back pressure until the
        * first token is set.
        */
      override def preStart(): Unit = {
        logger.debug("Initializing session flow.")
        import scala.concurrent.ExecutionContext.Implicits.global
        credentialsProvider.nextToken().onComplete(startGraph.invoke)
      }

      /**
        * Map serialized calls to [[HttpRequest]] with attached token.
        */
      setHandler(callsInlet, new InHandler {
        override def onPush(): Unit = {
          push(requestsOutlet, requestFactory(grab(callsInlet), token))
        }
      })

      /**
        * Forward pull or back pressure if no session token is available.
        */
      setHandler(requestsOutlet, new OutHandler {
        override def onPull(): Unit = {
          if (isInitialized) pull(callsInlet)
          else logger.debug("Received pull while not initialized.")
        }
      })
    }
  }
}
