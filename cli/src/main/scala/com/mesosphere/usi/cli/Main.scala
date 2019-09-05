package com.mesosphere.usi.cli
import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.{ResourceRequirement, ResourceType, ScalarRequirement}
import play.api.libs.json._

import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello")

    implicit val sys: ActorSystem = ActorSystem("UsiCli")
    implicit val mat: Materializer = ActorMaterializer()

    def maxLineSize: Int = 4096
    def maxEventSize: Int = 8192

    val stdinSource: Source[ByteString, Future[IOResult]] = StreamConverters.fromInputStream(() => System.in)
    val stdoutSink: Sink[ByteString, Future[IOResult]] = StreamConverters.fromOutputStream(() => System.out)

    val parser = EventStreamParser(maxLineSize, maxEventSize)

    stdinSource
      .via(parser)
      .map(parseCommand)
      .map { cmd =>
        println(s"Processing: $cmd")
        ByteString("done.")
      }
      .runWith(stdoutSink)
  }

  def parseCommand(event: ServerSentEvent): SchedulerCommand = {
    event.eventType
      .getOrElse(throw new IllegalArgumentException("Command did not include event type"))
      .toLowerCase match {
      case "launchpod" => Json.parse(event.data).as[LaunchPod]
      case "killpod" => Json.parse(event.data).as[KillPod]
      case other => throw new IllegalArgumentException(s"Unknown scheduler command '$other'")
    }
  }

  implicit val podIdRead: Reads[PodId] = Json.reads[PodId]
  implicit val resourceTypeRead: Reads[ResourceType] = Reads {
    case JsString(value) => JsSuccess(ResourceType.fromName(value))
    case other => JsError("unknown resource type")
  }
  implicit val uriRead: Reads[URI] = Reads {
    case JsString(value) => JsSuccess(new URI(value))
    case other => JsError("unknown uri type")
  }
  implicit val fetchUriRead: Reads[FetchUri] = Json.reads[FetchUri]
  implicit val resourceRequirementRead: Reads[ResourceRequirement] =
    Json.reads[ScalarRequirement].map(_.asInstanceOf[ResourceRequirement]) // TODO: support RangeRequirement
  implicit val runTemplateRead: Reads[RunTemplate] = Json.reads[RunTemplate]
  implicit val launchPodRead: Reads[LaunchPod] = Json.reads[LaunchPod]
  implicit val killPodRead: Reads[KillPod] = Json.reads[KillPod]
}
