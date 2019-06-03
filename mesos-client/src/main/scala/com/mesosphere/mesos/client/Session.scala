package com.mesosphere.mesos.client
import java.net.URL

import akka.NotUsed
import akka.actor.ActorSystem
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

case class Session(url: URL, streamId: String, authorization: Option[CredentialsProvider] = None) {
  lazy val isSecured: Boolean = url.getProtocol == "https"
  lazy val port = if (url.getPort == -1) url.getDefaultPort else url.getPort

  /**
    * Construct a new [[HttpRequest]] for a serialized Mesos call and a set of authorization, ie session token.
    * @param bytes The bytes of the serialized Mesos call.
    * @param maybeCredentials The session token if required.
    * @return The [[HttpRequest]] with proper headers and body.
    */
  def createPostRequest(bytes: Array[Byte], maybeCredentials: Option[HttpCredentials]): HttpRequest =
    HttpRequest(
      HttpMethods.POST,
      uri = Uri(s"${url.getPath}/api/v1/scheduler"),
      entity = HttpEntity(MesosClient.ProtobufMediaType, bytes),
      headers = MesosClient.MesosStreamIdHeader(streamId) :: maybeCredentials.map(Authorization(_)).toList
    )

  case class SessionFlow(credentialsProvider: CredentialsProvider)
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
            push(requestsOutlet, createPostRequest(grab(callsInlet), token))
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

  // A flow that transforms serialized Mesos calls to proper HTTP requests.
  val post: Flow[Array[Byte], HttpRequest, NotUsed] = RestartFlow.withBackoff(1.seconds, 1.seconds, 1)(() =>
    authorization match {
      case Some(credentialsProvider) => Flow.fromGraph(SessionFlow(credentialsProvider))
      case None => Flow[Array[Byte]].map(createPostRequest(_, None))
  })

  /** @return The connection pool for this session. */
  def connectionPool(implicit system: ActorSystem): Flow[HttpRequest, Try[HttpResponse], NotUsed] = {
    val poolSettings = ConnectionPoolSettings("").withMaxConnections(1).withPipeliningLimit(1)
    if (isSecured) {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().cachedHostConnectionPoolHttps(host = url.getHost, port = port, settings = poolSettings))
        .map(_._1)
    } else {
      Flow[HttpRequest]
        .map(_ -> NotUsed)
        .via(Http().cachedHostConnectionPool(host = url.getHost, port = port))
        .map(_._1)
    }
  }
}
