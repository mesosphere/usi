package com.mesosphere.usi.core.revive

import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.faultdomain.HomeRegionFilter
import com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory
import com.mesosphere.usi.core.models.{PodId, PodSpecUpdatedEvent, RunningPodSpec}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.metrics.DummyMetrics
import org.scalatest.Inside

import scala.concurrent.Future
import scala.concurrent.duration._

class SuppressReviveHandlerTest extends AkkaUnitTest with Inside {
  import SuppressReviveHandler.{RoleDirective, IssueUpdateFramework, IssueRevive}
  class Fixture(debounceReviveInterval: FiniteDuration = 1.seconds, defaultRole: String = "web") {

    val webApp = SimpleRunTemplateFactory(Nil, "", "web")
    val monitoringApp = SimpleRunTemplateFactory(Nil, "", "monitoring")

    val inputSourceQueue = Source.queue[PodSpecUpdatedEvent](16, OverflowStrategy.fail)
    val outputSinkQueue = Sink.queue[RoleDirective]()
    val podIdIndex = new AtomicInteger()
    def newPodId(prefix: String): PodId = PodId(s"prefix-${podIdIndex.incrementAndGet()}")
    val initialFrameworkInfo = MesosMock.mockFrameworkInfo(Seq("web"))
    val mesosCallFactory = new MesosCalls(MesosMock.mockFrameworkId)
    val reviveOffersStreamLogic = new SuppressReviveHandler(
      initialFrameworkInfo,
      MesosMock.mockFrameworkId,
      DummyMetrics,
      mesosCallFactory,
      debounceReviveInterval = debounceReviveInterval
    )
  }

  class NonDebouncedFixture(defaultRole: String = "web")
      extends Fixture(debounceReviveInterval = 0.seconds, defaultRole = defaultRole) {
    // Tests are more easily tested without time-dependent throttling logic
    val nonDebouncedSuppressReviveFlow: Flow[PodSpecUpdatedEvent, RoleDirective, NotUsed] =
      reviveOffersStreamLogic.reviveStateFromPodSpecs
        .via(reviveOffersStreamLogic.reviveDirectiveFlow)
  }

  "Suppress and revive logic" should {
    "combine 3 revive-worthy events received within the throttle window into a single revive event" in new Fixture(
      debounceReviveInterval = 200.millis,
      defaultRole = "web"
    ) {
      val podSpec1 = RunningPodSpec(newPodId("webapp"), webApp, HomeRegionFilter)
      val podSpec2 = RunningPodSpec(newPodId("webapp"), webApp, HomeRegionFilter)
      val podSpec3 = RunningPodSpec(newPodId("webapp"), webApp, HomeRegionFilter)

      Given("A suppress/revive flow with suppression enabled and 200 millis revive interval")
      val suppressReviveFlow = reviveOffersStreamLogic.suppressAndReviveFlow

      val (input, output) = inputSourceQueue.via(suppressReviveFlow).toMat(outputSinkQueue)(Keep.both).run

      When("3 new podSpec updates are sent for the role 'web'")
      Future
        .sequence(Seq(podSpec1, podSpec2, podSpec3).map { i =>
          input.offer(PodSpecUpdatedEvent.forUpdate(i))
        })
        .futureValue

      Then("The initial update framework message is emitted")
      // Note - we want to assert that this is emitted initially in order to create a clean-slate scenario with Mesos;
      // future updateFramework / revive calls are issues based on assumption of former state.
      inside(output.pull().futureValue) {
        case Some(IssueUpdateFramework(roleState, _, _)) =>
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

      "issues a suppress for the default role during initial materialization" in new NonDebouncedFixture() {
        val results = Source.empty
          .via(nonDebouncedSuppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(IssueUpdateFramework(roleState, newlyRevived, newlySuppressed)) =>
            roleState shouldBe Map("web" -> Set.empty)
            newlyRevived shouldBe Set.empty
            newlySuppressed shouldBe Set.empty
        }
      }

      "emit a revive for each new scheduled instance added, and suppresses once all instances are launched" in new NonDebouncedFixture() {
        val podId1 = newPodId("web")
        val podId2 = newPodId("web")

        val events = List(
          PodSpecUpdatedEvent.forUpdate(RunningPodSpec(podId1, webApp, HomeRegionFilter)),
          PodSpecUpdatedEvent.forUpdate(RunningPodSpec(podId2, webApp, HomeRegionFilter)),
          PodSpecUpdatedEvent(podId1, None),
          PodSpecUpdatedEvent(podId2, None)
        )
        val results = Source(events)
          .via(nonDebouncedSuppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(
                initialMessage: IssueUpdateFramework,
                reviveForPodSpec1: IssueRevive,
                reviveForPodSpec2: IssueRevive,
                finalSuppress: IssueUpdateFramework
              ) =>
            initialMessage.roleState shouldBe Map("web" -> Set.empty)

            reviveForPodSpec1.roles shouldBe Set("web")
            reviveForPodSpec2.roles shouldBe Set("web")

            finalSuppress.roleState shouldBe Map("web" -> Set.empty)
            finalSuppress.newlySuppressed shouldBe Set("web")
        }
      }

      "does not emit a new revive for updates to existing scheduled instances" in new NonDebouncedFixture() {
        val podSpec1 = RunningPodSpec(newPodId("web"), webApp, HomeRegionFilter)

        val results = Source(List(PodSpecUpdatedEvent.forUpdate(podSpec1), PodSpecUpdatedEvent.forUpdate(podSpec1)))
          .via(nonDebouncedSuppressReviveFlow)
          .runWith(Sink.seq)
          .futureValue

        inside(results) {
          case Seq(updateFramework: IssueUpdateFramework, reviveForFirstUpdate: IssueRevive) =>
            updateFramework.roleState shouldBe Map("web" -> Set.empty)

            reviveForFirstUpdate.roles shouldBe Set("web")
        }
      }
    }
  }
}
