package com.mesosphere.usi.core.integration

import akka.event.Logging
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.scaladsl.{Keep, Sink}
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.SchedulerFactory
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.commandInputSource
import com.mesosphere.usi.core.models.{PodId, PodStatus, PodStatusUpdatedEvent, StateEvent, TaskId}
import com.mesosphere.usi.core.models.commands.{ExpungePod, KillPod, LaunchPod, SchedulerCommand}
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory
import com.mesosphere.usi.core.util.DurationConverters
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import org.apache.mesos.v1.Protos
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.concurrent
import scala.collection.mutable
import scala.concurrent.duration._

class SchedulerFuzzingTest extends AkkaUnitTest with MesosClusterTest with Inside with ScalaCheckPropertyChecks {
  override def materializerSettings = super.materializerSettings.withDebugLogging(true)
  override implicit lazy val mat = ActorMaterializer()

  lazy val mesosClientSettings = MesosClientSettings.load().withMasters(Seq(mesosFacade.url))
  val frameworkInfo = Protos.FrameworkInfo
    .newBuilder()
    .setUser("test")
    .setName("SimpleHelloWorldExample")
    .addRoles("test")
    .addCapabilities(
      Protos.FrameworkInfo.Capability.newBuilder().setType(Protos.FrameworkInfo.Capability.Type.MULTI_ROLE))
    .build()

  "random commands" in {
    // Expected pod states
    val expectedState = mutable.Map[PodId, Protos.TaskState]()

    /**
      * Updated the expected state. It is called in the beginning of forAll.
      * @param cmd The next command.
      */
    def updateExpectedState(cmd: SchedulerCommand): Unit = cmd match {
      case launchPod: LaunchPod if wasNotKilled(launchPod.podId) =>
        // Pods can only become launching if they haven't run before.
        expectedState.update(launchPod.podId, Protos.TaskState.TASK_RUNNING)
      case killPod: KillPod if isKnownPod(killPod.podId) =>
        expectedState.update(killPod.podId, Protos.TaskState.TASK_KILLED)
      case expungePod: ExpungePod => expectedState.remove(expungePod.podId)
      case ExpungePod(podId) =>
        expectedState.remove(podId)
      case other =>
        logger.debug(s"Ignoring USI command. command=$other")
    }

    /** @return whether a pod exists in a killed state. */
    def wasNotKilled(id: PodId): Boolean = expectedState.get(id).exists(_ != Protos.TaskState.TASK_KILLED)

    /** @return whether the pod is known or unknown */
    def isKnownPod(id: PodId): Boolean = id.value.startsWith("random")

    // Observed pod states as reported by USI
    var observedState = concurrent.TrieMap.empty[PodId, Protos.TaskState]

    /**
      * Process [[StateEvent]]s from USI to replicated the internal USI pod record state.
      * @param update The update from USI. We only case for [[PodStatusUpdatedEvent]].
      */
    def updateObservedState(update: StateEvent): Unit = update match {
      case PodStatusUpdatedEvent(podId, None) =>
        logger.debug(s"Removing pod. podId=${podId.value}")
        observedState.remove(podId)
      case PodStatusUpdatedEvent(podId, Some(PodStatus(_, taskStatuses))) =>
        val state = taskStatuses(TaskId(podId.value)).getState
        logger.debug(s"Adding pod status. podId=${podId.value} state=$state")
        observedState.update(podId, state)
      case other => logger.debug(s"Ignoring USI state event. event=$other")
    }

    // Commands
    def genPodLaunches =
      for {
        //podId <- Arbitrary.arbitrary[String].suchThat(s => s.nonEmpty && s.matches("^[a-zA-Z0-9\\-\\.]+$"))
        podId <- Arbitrary.arbitrary[Int].map(i => PodId(s"random-pod-$i")).suchThat(id => !expectedState.contains(id))
        cpu <- Gen.choose(0.1, 1.1)
        mem <- Gen.choose(2, 256)
      } yield
        LaunchPod(
          podId,
          SimpleRunTemplateFactory(
            resourceRequirements = List(ScalarRequirement.cpus(cpu), ScalarRequirement.memory(mem)),
            shellCommand = "sleep 3600",
            "test")
        )

    // Gen.delay will evaluate expectedState each time.
    def genPodKills = Gen.delay {
      if (expectedState.nonEmpty) Gen.oneOf(expectedState.keys).map(KillPod)
      else Arbitrary.arbitrary[Int].map(id => KillPod(PodId(s"unknown-pod-$id")))
    }

    def genExpungePod = Gen.delay {
      if (expectedState.nonEmpty) Gen.oneOf(expectedState.keys).map(ExpungePod)
      else Arbitrary.arbitrary[Int].map(id => ExpungePod(PodId(s"unknown-pod-$id")))
    }

    def genCommands = Gen.frequency((50, genPodLaunches), (20, genPodKills), (5, genExpungePod))

    // Setup USI
    lazy val mesosClient: MesosClient = MesosClient(mesosClientSettings, frameworkInfo).runWith(Sink.head).futureValue
    val schedulerSettings = SchedulerSettings
      .load()
      .withDebounceReviveInterval(DurationConverters.toJava(50.millis))
    lazy val factory = SchedulerFactory(mesosClient, InMemoryPodRecordRepository(), schedulerSettings, DummyMetrics)
    lazy val (_, schedulerFlow) = factory.newSchedulerFlow().futureValue
    lazy val (input, output) = commandInputSource
      .log("scheduler commands")
      .via(schedulerFlow)
      .log("scheduler events")
      .toMat(Sink.foreach(updateObservedState))(Keep.both)
      .withAttributes(Attributes
        .logLevels(onElement = Logging.DebugLevel, onFinish = Logging.InfoLevel, onFailure = Logging.ErrorLevel))
      .run

    // Run commands
    forAll(genCommands) { cmd =>
      logger.debug(s"Running command. command=$cmd")

      updateExpectedState(cmd)

      // Apply command
      input.offer(cmd).futureValue

      // Assert
      eventually {
        logger.debug("Asserting observed and expected state.")
        observedState.snapshot() should contain theSameElementsAs expectedState
      }
    }

    // Assert the Mesos state
    val expectedRunning = expectedState.filter {
      case (_, state) => state == Protos.TaskState.TASK_RUNNING
    }
    val actualMesosState = mesosFacade
      .state()
      .value
      .frameworks
      .head
      .tasks
      .map { task =>
        PodId(task.id) -> Protos.TaskState.valueOf(task.state.value)
      }
      .toMap
    actualMesosState should contain theSameElementsAs expectedRunning

    // Stop
    mesosClient.killSwitch.shutdown()
    output.futureValue
  }
}
