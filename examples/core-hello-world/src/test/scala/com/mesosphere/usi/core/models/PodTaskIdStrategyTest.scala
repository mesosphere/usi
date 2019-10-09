package com.mesosphere.usi.core.models

import com.mesosphere.utils.UnitTest

class PodTaskIdStrategyTest extends UnitTest {
  "default strategy" should {
    Seq(
      (PodId("testing"), TaskName.empty) -> TaskId("testing"),
      (PodId("testing"), TaskName("web")) -> TaskId("testing.web"),
      (PodId("testing"), TaskName("web.1")) -> TaskId("testing.web.1")
    ).foreach {
      case ((podId, taskName), taskId) =>
        s"combine ${podId} and ${taskName} to ${taskId}" in {
          PodTaskIdStrategy.DefaultStrategy.apply(podId, taskName) shouldBe taskId
        }
        s"parse ${podId} and ${taskName} from ${taskId}" in {
          val Some((unappliedPodId, unappliedTaskName)) =
            PodTaskIdStrategy.DefaultStrategy.unapply(taskId)
          unappliedPodId shouldBe podId
          unappliedTaskName shouldBe taskName
        }
    }
  }
}
