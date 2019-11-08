package com.mesosphere.usi.core.revive

import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory
import com.mesosphere.usi.core.models.{PodId, PodSpecUpdatedEvent, RunningPodSpec}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.metrics.DummyMetrics
import org.scalatest.Inside

import scala.concurrent.Future
import scala.concurrent.duration._

class SuppressReviveHandlerTest extends AkkaUnitTest with Inside  {
  import SuppressReviveHandler.{RoleDirective, UpdateFramework, IssueRevive}
  val webApp = SimpleRunTemplateFactory(Nil, "", "web")
  val monitoringApp = SimpleRunTemplateFactory(Nil, "", "monitoring")

  val inputSourceQueue = Source.queue[PodSpecUpdatedEvent](16, OverflowStrategy.fail)
  val outputSinkQueue = Sink.queue[RoleDirective]()
  val podIdIndex = new AtomicInteger()
  def newPodId(prefix: String): PodId = PodId(s"prefix-${podIdIndex.incrementAndGet()}")
  val initialFrameworkInfo = MesosMock.mockFrameworkInfo
  val mesosCallFactory = new MesosCalls(MesosMock.mockFrameworkId)
  val reviveOffersStreamLogic = new SuppressReviveHandler(initialFrameworkInfo, DummyMetrics, mesosCallFactory, "web")

  "Suppress and revive logic" should {
    "combine 3 revive-worthy events received within the throttle window into a single revive event" in {
      val podSpec1 = RunningPodSpec(newPodId("webapp"), webApp)
      val podSpec2 = RunningPodSpec(newPodId("webapp"), webApp)
      val podSpec3 = RunningPodSpec(newPodId("webapp"), webApp)

      Given("A suppress/revive flow with suppression enabled and 200 millis revive interval")
      val suppressReviveFlow = reviveOffersStreamLogic.suppressAndReviveFlow(minReviveOffersInterval = 200.millis, defaultRole = "web")

      val (input, output) = inputSourceQueue.via(suppressReviveFlow).toMat(outputSinkQueue)(Keep.both).run

      When("3 new podSpec updates are sent for the role 'web'")
      Future.sequence(Seq(podSpec1, podSpec2, podSpec3).map { i =>
        input.offer(PodSpecUpdatedEvent.forUpdate(i))
      }).futureValue

      Then("The initial update framework message is emitted")
      // Note - we want to assert that this is emitted initially in order to create a clean-slate scenario with Mesos;
      // future updateFramework / revive calls are issues based on assumption of former state.
      inside(output.pull().futureValue) {
        case Some(UpdateFramework(roleState, _, _)) =>
        roleState("web").shouldBe(Set.empty)
      }

      Then("The revives from the instances get combined in to a single update framework call")
      inside(output.pull().futureValue) {
        case Some(IssueRevive(roles)) =>
          roles shouldBe Set("web")
      }

      When("the stream is closed")
      input.complete()

      Then("no further events are emitted")
      output.pull().futureValue shouldBe None // should be EOS
    }

    "Suppress and revive (without debouncing)" should {
      // Many of these components are more easily tested without throttling logic
      val suppressReviveFlow: Flow[PodSpecUpdatedEvent, RoleDirective, NotUsed] =
        reviveOffersStreamLogic
          .reviveStateFromPodSpecs
          .via(reviveOffersStreamLogic.reviveDirectiveFlow)

      "issues a suppress for the default role during initial materialization" in {
        val results = Source.empty
          .via(suppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(UpdateFramework(roleState, newlyRevived, newlySuppressed)) =>
            roleState shouldBe Map("web" -> Set.empty)
            newlyRevived shouldBe Set.empty
            newlySuppressed shouldBe Set.empty
        }
      }

      "emit a revive for each new scheduled instance added" in {
        val podSpec1 = RunningPodSpec(newPodId("web"), webApp)
        val podSpec2 = RunningPodSpec(newPodId("web"), webApp)

        val results = Source(
          List(
            PodSpecUpdatedEvent.forUpdate(podSpec1),
            PodSpecUpdatedEvent.forUpdate(podSpec2)))
          .via(suppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(initialMessage: UpdateFramework, reviveForPodSpec1: IssueRevive, reviveForPodSpec2: IssueRevive) =>
            initialMessage.roleState shouldBe Map("web" -> Set.empty)

            reviveForPodSpec1.roles shouldBe Set("web")
            reviveForPodSpec2.roles shouldBe Set("web")
        }
      }

      "does not emit a new revive for updates to existing scheduled instances" in {
        val podSpec1 = RunningPodSpec(newPodId("web"), webApp)

        val results = Source(
          List(
            PodSpecUpdatedEvent.forUpdate(podSpec1),
            PodSpecUpdatedEvent.forUpdate(podSpec1)))
          .via(suppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(updateFramework: UpdateFramework, reviveForFirstUpdate: IssueRevive) =>
            updateFramework.roleState shouldBe Map("web" -> Set.empty)

            reviveForFirstUpdate.roles shouldBe Set("web")
        }
      }
    }
  }
}
