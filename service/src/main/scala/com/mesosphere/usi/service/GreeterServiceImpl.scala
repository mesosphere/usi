package com.mesosphere.usi.service
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import com.mesosphere.usi.core.{Scheduler, models}

import scala.concurrent.Future

class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService with StrictLogging {
  import mat.executionContext

  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    logger.info(s"sayHello to ${in.name}")
    Future.successful(HelloReply(s"Hello, ${in.name}"))
  }

  override def itKeepsTalking(in: Source[HelloRequest, NotUsed]): Future[HelloReply] = {
    logger.info(s"sayHello to in stream...")
    in.runWith(Sink.seq).map(elements => HelloReply(s"Hello, ${elements.map(_.name).mkString(", ")}"))
  }

  override def itKeepsReplying(in: HelloRequest): Source[HelloReply, NotUsed] = {
    logger.info(s"sayHello to ${in.name} with stream of chars...")
    Source(s"Hello, ${in.name}".toList).map(character => HelloReply(character.toString))
  }

  override def streamHellos(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {
    logger.info(s"sayHello to stream...")
    in.map(request => HelloReply(s"Hello, ${request.name}"))
  }

  override def setup(in: Source[LaunchPod, NotUsed]): Source[PodStateEvent, NotUsed] = {
    val (_, schedulerFlow) = Scheduler.fromClient(???, ???, ???).value.get.get
    in.map(protobufToInternal).via(schedulerFlow).map(internalToProto)
  }

  def protobufToInternal(proto: LaunchPod): models.LaunchPod = {
    val runSpec = models.RunTemplate(
      resourceRequirements = Seq.empty,
      shellCommand = proto.runSpec.get.shellCommand,
      role = proto.runSpec.get.role)
    models.LaunchPod(models.PodId(proto.podId), runSpec)
  }

  def internalToProto(event: models.StateEvent): PodStateEvent = {
    ???
  }
}
