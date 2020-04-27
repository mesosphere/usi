package com.mesosphere.usi.core.integration

import akka.event.Logging
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, Attributes, QueueOfferResult}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.SchedulerFactory
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.commandInputSource
import com.mesosphere.usi.core.models.commands.LaunchPod
import com.mesosphere.usi.core.models.resources.{RangeRequirement, ScalarRequirement}
import com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory
import com.mesosphere.usi.core.models.{commands, _}
import com.mesosphere.usi.core.util.DurationConverters
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.scalatest.Inside

import scala.concurrent.duration._
import scala.util.Try

class SchedulerIntegrationTest extends AkkaUnitTest with MesosClusterTest with Inside {
  override def materializerSettings = super.materializerSettings.withDebugLogging(true)
  override implicit lazy val mat = ActorMaterializer()

  lazy val mesosClientSettings = MesosClientSettings.load().withMasters(Seq(mesosFacade.url))
  val frameworkInfo = Protos.FrameworkInfo
    .newBuilder()
    .setUser("test")
    .setName("SimpleHelloWorldExample")
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .build()

  lazy val mesosClient: MesosClient = MesosClient(mesosClientSettings, frameworkInfo).runWith(Sink.head).futureValue
  val schedulerSettings = SchedulerSettings
    .load()
    .withDebounceReviveInterval(DurationConverters.toJava(50.millis))
  lazy val factory = SchedulerFactory(mesosClient, InMemoryPodRecordRepository(), schedulerSettings, DummyMetrics)
  lazy val (snapshot, schedulerFlow) =
    factory.newSchedulerFlow().futureValue
  lazy val (input, output) = commandInputSource
    .log("scheduler commands")
    .via(schedulerFlow)
    .log("scheduler events")
    .toMat(Sink.queue())(Keep.both)
    .withAttributes(Attributes
      .logLevels(onElement = Logging.DebugLevel, onFinish = Logging.InfoLevel, onFailure = Logging.ErrorLevel))
    .run

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "launches a pod with Mesos and reports the status" in {
    val podId = PodId("scheduler-integration-test-pod")

    input.offer(
      LaunchPod(
        podId,
        SimpleRunTemplateFactory(
          resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
          shellCommand = "sleep 3600",
          "test")
      ))

    inside(output.pull().futureValue) {
      case Some(specUpdated: PodSpecUpdatedEvent) =>
        specUpdated.id shouldBe podId
    }

    inside(output.pull().futureValue) {
      case Some(recordUpdated: PodRecordUpdatedEvent) =>
        recordUpdated.id shouldBe podId
    }
    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdatedEvent) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }

  "launches pod with ports" in {
    val podId = PodId("pod-with-ports")

    input.offer(
      commands.LaunchPod(
        podId,
        SimpleRunTemplateFactory(
          resourceRequirements =
            List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256), RangeRequirement.ports(Seq(0))),
          shellCommand = "sleep 3600",
          "test"
        )
      ))

    eventually {
      inside(output.pull().futureValue) {
        case Some(podStatusChange: PodStatusUpdatedEvent) =>
          podStatusChange.newStatus.get
            .taskStatuses(TaskId(podId.value))
            .getState shouldBe Protos.TaskState.TASK_RUNNING
      }
    }
  }

  "terminates the stream when Mesos master dies" in {
    Given("A Mesos client")
    lazy val mesosClient: MesosClient = MesosClient(mesosClientSettings, frameworkInfo).runWith(Sink.head).futureValue
    lazy val factory = SchedulerFactory(mesosClient, InMemoryPodRecordRepository(), schedulerSettings, DummyMetrics)
    lazy val (_, schedulerFlow) = factory.newSchedulerFlow().futureValue
    lazy val (input, output) = commandInputSource
      .log("scheduler commands")
      .via(schedulerFlow)
      .log("scheduler events")
      .toMat(Sink.queue())(Keep.both)
      .withAttributes(Attributes
        .logLevels(onElement = Logging.DebugLevel, onFinish = Logging.InfoLevel, onFailure = Logging.ErrorLevel))
      .run

    And("a first successful command.")
    input.offer(commands.KillPod(PodId("unknown-pod-1"))).futureValue
    inside(output.pull().futureValue.value) {
      case PodSpecUpdatedEvent(PodId(idString), _) =>
        idString should be("unknown-pod-1")
    }

    When("Mesos masters crash.")
    logger.info("Kill Mesos masters.")
    mesosCluster.masters.foreach(_.stop())

    Then("The stream terminates")
    input.watchCompletion().futureValue

    eventually {
      val pullResult = Try(output.pull().futureValue)
      pullResult.isFailure should be(true)
    }
  }
}
