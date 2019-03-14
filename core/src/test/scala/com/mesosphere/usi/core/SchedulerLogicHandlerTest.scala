package com.mesosphere.usi.core

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.{Goal, PodId, PodSpec, PodSpecUpdated, RunSpec}
import com.mesosphere.utils.UnitTest

class SchedulerLogicHandlerTest extends UnitTest {
  "output a mesos call with revive for new podspec role" in {
    Given("Scheduler logic handler with empty state")
    val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId))
    val podId = PodId("pod")
    When("pod with role 'test-role' is updated")
    val result = handler.handleSpecEvent(
      PodSpecUpdated(podId, Some(PodSpec(podId, Goal.Running, RunSpec(Seq.empty, "", "test-role")))))
    Then("revive call is generated for that role")
    result.mesosCalls.exists(c => c.hasRevive && c.getRevive.getRoles(0) == "test-role") should be(true) withClue s"Expecting revive call with role 'test-role' but got ${result.mesosCalls}"

    And("Another revive is generated for the same role when another podspec is updated")
    val podId2 = PodId("pod2")
    val result2 = handler.handleSpecEvent(
      PodSpecUpdated(podId2, Some(PodSpec(podId2, Goal.Running, RunSpec(Seq.empty, "", "test-role")))))
    result2.mesosCalls.exists(c => c.hasRevive && c.getRevive.getRoles(0) == "test-role") should be(true) withClue s"Expecting revive call with role 'test-role' but got ${result.mesosCalls}"

  }
}