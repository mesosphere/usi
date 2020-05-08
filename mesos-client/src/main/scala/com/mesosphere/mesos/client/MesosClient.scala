package com.mesosphere.mesos.client

import java.io.IOException
import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaType.Compressible
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl._
import akka.stream.{Materializer, OverflowStrategy, _}
import akka.util.{ByteString, Timeout}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import com.mesosphere.mesos.conf.{MesosClientSettings, SemanticVersion}
import org.apache.mesos.v1.Protos.{FrameworkID, FrameworkInfo, MasterInfo}
import org.apache.mesos.v1.scheduler.Protos.{Call, Event}

import scala.concurrent.Future

trait MesosClient {

  /**
    * The frameworkId as which this client is currently connected.
    */
  def frameworkId: FrameworkID

  /**
    * The framework info this client is currently subscribed with.
    */
  def frameworkInfo: FrameworkInfo

  /**
    * The information about the current connection to Mesos Master.
    *
    * Note: MesosClient will disconnect on Mesos Master failover. It is the responsibility of the consumer as such to
    * reconnect to Mesos in such an event. As such, this information will be current, so long as we are connected.
    */
  def session: Session

  /**
    * Calling `shutdown()` or `abort()` on this will close both the original connection to Mesos and all event publisher
    * connections created by materializing mesosSink.
    *
    * Note that depending on `failoverTimeout` provided with SUBSCRIBED call, Mesos could start killing tasks and
    * executors started by the framework. Make sure to set `failoverTimeout` appropriately.
    *
    * See `teardown()` Call factory method for another way to shutdown a framework.
    **/
  def killSwitch: KillSwitch

  /**
    * Materializable-once source containing a stream of events from the currently connected Mesos Master.
    *
    * This stream will terminate if the connection is lost the Mesos Master. There are no attempts to automatically
    * handle reconnection at this layer.
    */
  def mesosSource: Source[Event, NotUsed]

  /**
    * Akka Sink that is used to publish events to the current connected Mesos Master.
    *
    * This sink can be materialized multiple times, with each stream creating a single new HTTP connection to
    * Mesos. Message-order delivery to Mesos is preserved at a stream.
    *
    * If you would like to have multiple streams share the same new HTTP connection, consider using see MergeHub,
    * FlowOps.merge, or GraphDSL Merge node.
    *
    * The flow visualized:
    *
    * |  |  |
    * v  v  v
    * +------------+
    * | Event      |
    * | Serializer | (1)
    * +------------+
    * |
    * v
    * +------------+
    * | Request    |
    * | Builder    | (2)  <-- reads Mesos StreamId and from connection context and adds the Framework ID.
    * +------------+
    * |
    * v
    * +------------+
    * | Http       |
    * | Connection | (3)  <-- reads Mesos URL from connection context
    * +------------+
    * |
    * v
    * +------------+
    * | Response   |
    * | Handler    | (4)
    * +------------+
    *
    * 1. Event Serializer serializes calls to byte array
    * 2. Build a HTTP request from the data using `mesosStreamId` header from the context
    * 3. Http connection uses akka's `Http().outgoingConnection` to sends the data to mesos. Note that all calls are sent
    * through one long-living connection.
    * 4. Response handler will discard response entity or throw an exception on non-2xx response code
    *
    * Note: the materialized Future[Done] will be completed (either successfully, or with an error) if the connection to
    * the Mesos Master is lost. Any pending messages in flight (in the stream, or transmitting over TCP) before this
    * connection is lost are dropped. Usually, when this happens, the `mesosSource` will also drop, although you should
    * not always depend on this. It is the recommendation that if either the `mesosSink` or the `mesosSource` streams
    * terminate, for any reason, that the entire MesosClient is terminated.
    */
  def mesosSink: Sink[Call, Future[Done]]

  def masterInfo: MasterInfo
}

object MesosClient extends StrictLogging with StrictLoggingFlow {
  case class MesosRedirectException(leader: Uri) extends Exception(s"New mesos leader available at $leader")

  val MesosStreamIdHeaderName = "Mesos-Stream-Id"
  def MesosStreamIdHeader(streamId: String) =
    headers.RawHeader("Mesos-Stream-Id", streamId)
  val ProtobufMediaType: MediaType.Binary =
    MediaType.applicationBinary("x-protobuf", Compressible)

  /**
    * This is the first step in the communication process between the scheduler and the master. This is also to be
    * considered as subscription to the "/scheduler" event stream. To subscribe with the master, the scheduler sends
    * an HTTP POST with a SUBSCRIBE message including the required FrameworkInfo. Note that if
    * `subscribe.framework_info.id` is not set, master considers the scheduler as a new one and subscribes it by
    * assigning it a FrameworkID. The HTTP response is a stream in RecordIO format; the event stream begins with a
    * SUBSCRIBED event.
    *
    * Note: this method is used by mesos client to establish connection to mesos master and is not supposed to be called
    * directly by the framework.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1
    */
  private def newSubscribeCall(frameworkInfo: FrameworkInfo): Call = {
    val b =
      Call
        .newBuilder()
        .setType(Call.Type.SUBSCRIBE)
        .setSubscribe(Call.Subscribe.newBuilder().setFrameworkInfo(frameworkInfo))

    if (frameworkInfo.hasId) b.setFrameworkId(frameworkInfo.getId)

    b.build()
  }

  private val eventDeserializer: Flow[ByteString, Event, NotUsed] =
    Flow[ByteString].map(bytes => Event.parseFrom(bytes.toArray))

  private def connectionSource(
      frameworkInfo: FrameworkInfo,
      uri: Uri,
      authorization: Option[CredentialsProvider],
      redirectsLeft: Int)(implicit as: ActorSystem): Source[HttpResponse, NotUsed] = {
    val body = newSubscribeCall(frameworkInfo).toByteArray

    def createPostRequest(bytes: Array[Byte], maybeCredentials: Option[HttpCredentials]): HttpRequest =
      HttpRequest(
        HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(ProtobufMediaType, bytes),
        headers = headers.Accept(ProtobufMediaType) :: maybeCredentials.map(Authorization(_)).toList
      )

    val httpConnection =
      if (Session.isSecured(uri)) Http().outgoingConnectionHttps(uri.authority.host.address(), uri.effectivePort)
      else Http().outgoingConnection(uri.authority.host.address(), uri.effectivePort)

    val requestSource = authorization match {
      case Some(provider) =>
        Source.fromFuture(provider.nextToken()).map { token =>
          createPostRequest(body, Some(token))
        }
      case None => Source.single(createPostRequest(body, None))
    }

    requestSource
      .via(info(s"Connecting to the new leader: $uri "))
      .via(httpConnection)
      .flatMapConcat { response: HttpResponse =>
        if (response.status.isRedirection()) {
          if (redirectsLeft > 0) {
            val locationHeader = response.header[headers.Location].get
            val redirectUri = locationHeader.uri.resolvedAgainst(uri)
            logger.warn(s"Redirect Mesos client from $uri to $redirectUri given header '$locationHeader'")
            response.discardEntityBytes()

            if (redirectUri == uri) {
              throw new IOException(s"Failed to connect to Mesos: Circular redirection for $uri.")
            }
            connectionSource(frameworkInfo, redirectUri, authorization, redirectsLeft - 1)
          } else {
            throw new IOException("Failed to connect to Mesos: Too many redirects.")
          }
        } else Source.single(response)
      }
      .via(info("HttpResponse: "))
  }

  private def mesosHttpConnection(
      frameworkInfo: FrameworkInfo,
      conf: MesosClientSettings,
      authorization: Option[CredentialsProvider])(
      implicit mat: Materializer,
      as: ActorSystem,
      askTimeout: Timeout): Source[(HttpResponse, Session), NotUsed] = {
    mesosHttpConnection(frameworkInfo, conf.masters.toList, conf.maxRedirects, authorization)
  }

  private def mesosHttpConnection(
      frameworkInfo: FrameworkInfo,
      urls: List[URL],
      maxRedirects: Int,
      authorization: Option[CredentialsProvider])(
      implicit mat: Materializer,
      as: ActorSystem,
      askTimeout: Timeout): Source[(HttpResponse, Session), NotUsed] =
    urls match {
      case Nil => throw new IOException(s"Failed to connect to Mesos: List of master urls exhausted.")
      case baseUrl :: rest =>
        // Convert the Java URL to an Akka Uri.
        val baseUri = Uri.from(
          scheme = baseUrl.getProtocol,
          host = baseUrl.getHost,
          port = if (baseUrl.getPort == -1) baseUrl.getDefaultPort else baseUrl.getPort)
        logger.info(s"Connecting to Mesos master $baseUri")
        val requestUri = baseUri.withPath(Path("/api/v1/scheduler"))
        connectionSource(frameworkInfo, requestUri, authorization, maxRedirects).map { response =>
          response.status match {
            case StatusCodes.OK =>
              logger.info(s"Connected successfully to $baseUri");
              val streamId = response.headers
                .find(h => h.is(MesosStreamIdHeaderName.toLowerCase))
                .getOrElse(throw new IllegalStateException(s"Missing MesosStreamId header in ${response.headers}"))

              (response, Session(baseUri, streamId.value(), authorization))
            case _ =>
              response.discardEntityBytes()
              throw new IllegalArgumentException(s"Mesos server error: ${response.status}")
          }
        }.recoverWithRetries(
          1, {
            case ex if rest.nonEmpty =>
              // TODO: This retry only works on the initial connection. It does *not* work the the connection dies.
              logger.warn(s"Failed to connect to Mesos $baseUri", ex)
              mesosHttpConnection(frameworkInfo, rest, maxRedirects, authorization)
          }
        )
    }

  /**
    * Input events (Call) are sent to the scheduler, serially, with backpressure. Events received from Mesos are
    * received accordingly.
    */
  /**
    * Returns an Akka Stream Source which yields a single MesosClient.
    *
    * The mesosSource method on the returned client will be closed either on connection error or connection shutdown,
    * e.g.:
    *
    * ```
    * client.mesosSource.runWith(Sink.ignore).onComplete{
    *   case Success(res) => logger.info(s"Stream completed: $res")
    *   case Failure(e) => logger.error(s"Error in stream: $e")
    * }
    * ```
    *
    * No attempt is made to handle any reconnection logic after the Mesos Master connection is established. The client
    * is expected to handle disconnects and re-instantiate the Mesos Client as needed.
    *
    * The basic flow for connecting to Mesos and reading events looks some like this:
    *
    * +------------+
    * | Http       | (1)
    * | Connection |
    * +------------+
    * |
    * v
    * +---------------+
    * | Http Response | (2)
    * | Bytes         |
    * +---------------+
    * |
    * v
    * +------------+
    * | RecordIO   | (3)
    * | Scanner    |
    * +------------+
    * |
    * v
    * +--------------+
    * | Event        | (4)
    * | Deserializer |
    * +--------------+
    * |
    * v
    * +------------+
    * | Subscribed | (5)
    * | Consumer   |
    * +------------+
    * v
    * +---------------+
    * | MesosClient   |
    * | (with events) | (6)
    * +---------------+
    *
    * 1. Http Connection: mesos-v1-client uses the Akka-http low-level `Http.outgoingConnection()` to `POST` a
    *    [SUBSCRIBE](http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1) request to Mesos
    *    `api/v1/scheduler` endpoint, providing framework info as requested. The HTTP response is a stream in RecordIO
    *    format which is handled by the later stages.
    *
    *    If we connect to a non-leader Mesos master, we automatically follow the redirect up to `conf.maxRedirects`.
    *
    *    When we receive the HTTP response headers from the master connection, we set aside the `Mesos-Stream-Id`(see
    *    the description of the
    *    [SUBSCRIBE](http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1) call) for later use
    *    in mesosClient.mesosSink. Schedulers are expected to make HTTP requests to the leading master. If requests are
    *    made to a non-leading master a `HTTP 307 Temporary Redirect` will be received with the `Location` header
    *    pointing to the leading master.
    *
    * 2. HTTP Response Bytes: The Akka HTTP response includes an Akka Stream for reading the HTTP response data. We
    *    flatten this stream of bytes into this stream such that down-stream components get blocks of ByteStrings.
    *
    * 3. RecordIO Scanner: Each stream message is encoded in RecordIO format, which essentially prepends to a single
    *    record (either JSON or serialized protobuf) its length in bytes: `[<length>\n<json string|protobuf
    *    bytes>]`. More about the format
    *    [here](http://mesos.apache.org/documentation/latest/scheduler-http-api/#recordio-response-format-1). RecordIO
    *    Scanner uses `RecordIOFraming.Scanner` from the [alpakka-library](https://github.com/akka/alpakka) to parse the
    *    extracted bytes into a complete message frame.
    *
    * 4. Event Deserializer: Currently mesos-v1-client only supports protobuf encoded events/calls. Event deserializer
    *    uses [scalapb](https://scalapb.github.io/) library to parse the extracted RecordIO frame from the previous
    *    stage into a mesos
    *    [Event](https://github.com/apache/mesos/blob/master/include/mesos/scheduler/scheduler.proto#L36)
    *
    * 5. Subscribed Handler: Consume a single `SUBSCRIBED` event, and instiates the MesosClient
    *
    * 6. MesosClient: Contains the subscribe information, connection information, etc. The events at this point are
    *    available via the materializable-once source, `.mesosSource`, which DOES NOT include the earlier-consumed
    *    SUBSCRIBED event.
    */
  def apply(conf: MesosClientSettings, frameworkInfo: FrameworkInfo, authorization: Option[CredentialsProvider] = None)(
      implicit
      system: ActorSystem,
      materializer: Materializer): Source[MesosClient, NotUsed] = {

    if (authorization.nonEmpty) {
      require(frameworkInfo.hasPrincipal, "The framework info must have a principal set if authorization is used.")
    }

    implicit val askTimeout = Timeout(conf.callTimeout)
    val httpConnection: Source[(HttpResponse, Session), NotUsed] =
      mesosHttpConnection(frameworkInfo, conf, authorization)

    val eventReader = Flow[ByteString]
      .via(RecordIOFraming.scanner())
      .via(eventDeserializer)
      .via(debug("Received mesos Event: "))
      .idleTimeout(conf.idleTimeout)
      .buffer(conf.sourceBufferSize, OverflowStrategy.backpressure)

    httpConnection.flatMapConcat {
      case (httpResponse, session) =>
        val sharedKillSwitch =
          KillSwitches.shared(s"MesosClient-${session.baseUri}")
        httpResponse.entity.withoutSizeLimit.dataBytes
          .via(eventReader)
          .via(sharedKillSwitch.flow)
          .prefixAndTail(1)
          .map {
            case (Seq(subscribedEvent), events) if subscribedEvent.getType == Event.Type.SUBSCRIBED =>
              val subscribed = subscribedEvent.getSubscribed
              new MesosClientImpl(
                frameworkInfo,
                sharedKillSwitch,
                subscribed,
                session,
                events.log("mesosSource events"))
            case (other, _) =>
              throw new RuntimeException(s"Expected subscribed event, got $other")
          }
    }
  }
}

/**
  *
  */
class MesosClientImpl(
    val frameworkInfo: FrameworkInfo,
    sharedKillSwitch: SharedKillSwitch,
    val subscribed: Event.Subscribed,
    val session: Session,
    /**
      * Events from Mesos scheduler, sans initial Subscribed event.
      */
    val mesosSource: Source[Event, NotUsed])(implicit as: ActorSystem, m: Materializer)
    extends MesosClient
    with StrictLoggingFlow {

  val frameworkId = subscribed.getFrameworkId

  val masterInfo = subscribed.getMasterInfo

  val minimalVersion = SemanticVersion(1, 9, 0)
  val version = SemanticVersion(masterInfo.getVersion).get
  require(
    version >= minimalVersion,
    s"Mesos master version $version is not compatible with required version $minimalVersion.")

  override def killSwitch: KillSwitch = sharedKillSwitch

  private val responseHandler: Sink[(HttpResponse, Call.Type), Future[Done]] =
    Sink.foreach[(HttpResponse, Call.Type)] {
      case (response, callType) =>
        if (response.status.isFailure()) {
          logger.info(s"A request to Mesos failed with response: ${response.status}")
          response.discardEntityBytes()
          throw new IllegalStateException(s"Failed to send $callType call to Mesos.")
        } else {
          logger.debug(s"Mesos $callType call response: $response")
          response.discardEntityBytes()
        }
    }

  private val callSerializer: Flow[Call, (Array[Byte], Call.Type), NotUsed] = Flow[Call]
    .map(call => call.toBuilder.setFrameworkId(frameworkId).build())
    .map(call => call.toByteArray -> call.getType)

  override val mesosSink: Sink[Call, Future[Done]] =
    Flow[Call]
      .via(sharedKillSwitch.flow[Call])
      .log("mesosSink calls")
      .via(callSerializer)
      .via(session.post[Call.Type].asFlow)
      .toMat(responseHandler)(Keep.right)
}
