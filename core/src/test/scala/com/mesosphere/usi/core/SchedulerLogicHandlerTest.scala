package com.mesosphere.usi.core

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.models.{Goal, PodId, PodSpec, PodSpecUpdated, RunSpec}
import com.mesosphere.utils.UnitTest

class SchedulerLogicHandlerTest extends UnitTest {
  "output a mesos call with revive for new podspec role" in {
    val handler = new SchedulerLogicHandler(new MesosCalls(MesosMock.mockFrameworkId))
    val podId = PodId("pod")
    val result = handler.handleSpecEvent(PodSpecUpdated(podId, Some(PodSpec(podId, Goal.Running, RunSpec(Seq.empty, "", Seq("test-role"))))))
    result.mesosCalls.exists(c => c.hasRevive && c.getRevive.getRoles(0) == "test-role") should be (true) withClue s"Expecting revive call with role 'test-role' but got ${result.mesosCalls}"
  }
}
