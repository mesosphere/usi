package com.mesosphere.mesos.examples

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.mesosphere.mesos.client.{MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Run a mesos-client example framework that:
  *  - uses only the raw mesos-client
  *  - successfully subscribes to Mesos master
  *  - starts one `echo "Hello, world" && sleep 3600` task
  *  - exits should the task fail (or fail to start)
  *
  *  Good to test against local Mesos.
  *
  */
class SimpleHelloWorldFramework(conf: Config) extends StrictLoggingFlow {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  /**
    * Framework info
    */
  val frameworkInfo = ProtosHelper.frameworkInfo(user = "test", name = "SimpleHelloWorldExample").build()

  /**
    * Mesos client and its settings. We wait for the client to connect to Mesos for 10 seconds. If it can't
    * the framework will exit with a [[java.util.concurrent.TimeoutException]]
    */
  val settings = MesosClientSettings(conf)
  val client = Await.result(MesosClient(settings, frameworkInfo).runWith(Sink.head), 10.seconds)

  logger.info(s"""Successfully subscribed to Mesos:
                 | Framework Id: ${client.frameworkId.getValue}
                 | Mesos host: ${client.connectionInfo.url}
       """.stripMargin)

  /**
    * Main event processor for all Mesos [[org.apache.mesos.v1.scheduler.Protos.Event]]s.
    */
  val eventProcessor = new EventProcessor(client.calls)

  /**
    * This is the main framework loop:
    *
    * +--------------+
    * |              |
    * | Mesos source |
    * |              |
    * +------+-------+
    *        |
    *        |
    *        | Events (1)
    *        |
    *        v
    * +------+--------------------------+
    * |                                 |
    * |   Event Handler                 |
    * |                                 |
    * |    - Accept or decline offers   | (2)
    * |    - Acknowledge task status    |
    * |      updates                    |
    * |                                 |
    * +------+--------------------------+
    *        |
    *        |
    *        | Calls (3)
    *        |
    *        |
    * +------v-------+
    * |              |
    * |  Mesos sink  |
    * |              |
    * +--------------+
    *
    * 1. Once connected to Mesos we start receiving Mesos [[org.apache.mesos.v1.scheduler.Protos.Event]]s from [[MesosClient.mesosSource]]
    *
    * 2. An event handler that currently handles two event types:
    *
    * [[org.apache.mesos.v1.Protos.Offer]]s:
    *    - if an offer have enough resources we send Mesos an [[org.apache.mesos.v1.scheduler.Protos.Call.Accept]]
    *      call with task details.
    *    - if not enough resources or no need to launch anything, we decline the offer by sending a
    *    [[org.apache.mesos.v1.scheduler.Protos.Call.Decline]]
    *
    * [[org.apache.mesos.v1.scheduler.Protos.Event.Update]]s:
    *    - If the task is happily running, we acknowledge the status update
    *    - Any other status will lead to framework termination
    *
    * 3. The above stage can produce zero or more Mesos [[org.apache.mesos.v1.scheduler.Protos.Call]]s which then are sent using [[MesosClient.mesosSink]]
    *
    */
  client
    .mesosSource
    .statefulMapConcat(() => {

      // Task state. This variable is overridden when task state changes e.g. task is being launched or received
      // new task status. Task specification uses smallest possible amount of resources and prints
      // "Hello, world" and sleeps for an hour.
      var task: Task = new Task(
        Spec(
          name = "hello-world",
          cmd = """echo "Hello, world" && sleep 3600""",
          cpus = 0.1,
          mem = 32.0
        ))

      event =>
        val (newTask, calls) = eventProcessor.process(task, event)
        task = newTask
        calls
    })
    .runWith(client.mesosSink)
    .onComplete {
      case Success(res) =>
        logger.info(s"Stream completed: $res");
        system.terminate()
      case Failure(e) =>
        logger.error(s"Error in stream: $e");
        system.terminate()
    }
}

object SimpleHelloWorldFramework {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("mesos-client")
    SimpleHelloWorldFramework(conf)
  }

  def apply(conf: Config): SimpleHelloWorldFramework = new SimpleHelloWorldFramework(conf)
}
