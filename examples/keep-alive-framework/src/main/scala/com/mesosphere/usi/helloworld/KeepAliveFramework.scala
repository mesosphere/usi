package com.mesosphere.usi.helloworld

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub}
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.helloworld.runspecs.InMemoryDemoRunSpecService
import com.mesosphere.usi.helloworld.http.Routes
import com.mesosphere.usi.helloworld.keepalive.KeepAliveWatcher
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class KeepAliveFramework(conf: Config) extends StrictLogging {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val client = new KeepAliveMesosClientFactory(conf).client

  val podRecordRepository = InMemoryPodRecordRepository()


  val (stateSnapshot, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, client, podRecordRepository)

  val sharableSink = MergeHub.source.to(sink).run()

  val sharableSource = source.toMat(BroadcastHub.sink)(Keep.right).run()


  val appsService = new InMemoryDemoRunSpecService(sharableSink, sharableSource)

  val keepAliveFlow = new KeepAliveWatcher(appsService)

  val stopped = Promise[Done]()

  sharableSource
    .watchTermination() { (m , f) => f.onComplete(stopped.complete); m}
    .via(keepAliveFlow.flow)
    .to(sharableSink)
    .run()

  val routes = new Routes(appsService)

  Http().bindAndHandle(routes.root, "localhost", 8080)

  Await.result(stopped.future, Duration.Inf)

  system.terminate()

}

object KeepAliveFramework {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("mesos-client").withFallback(ConfigFactory.load())
    KeepAliveFramework(conf)
  }

  def apply(conf: Config): KeepAliveFramework = new KeepAliveFramework(conf)
}
