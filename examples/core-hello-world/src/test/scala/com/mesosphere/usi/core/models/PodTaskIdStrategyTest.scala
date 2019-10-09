package com.mesosphere.usi.core.models

import com.mesosphere.utils.UnitTest

class PodTaskIdStrategyTest extends UnitTest {
  "default strategy" should {
    "when dealing with TaskName.empty" should {
      "returns a TaskId with the podId verbatim" in {
        PodTaskIdStrategy.DefaultStrategy.apply(PodId("testing"), TaskName.empty) shouldBe TaskId("testing")
      }

      "unapplies returning an empty taskName" in {
        val Some((unappliedPodId, unappliedTaskName)) =
          PodTaskIdStrategy.DefaultStrategy.unapply(TaskId("testing"))
        unappliedPodId shouldBe PodId("testing")
        unappliedTaskName shouldBe TaskName.empty
      }
    }

    "when dealing with a non-empty TaskName" should {
      "return the PodId and TaskId joined by a '.'" in {
        PodTaskIdStrategy.DefaultStrategy.apply(PodId("testing"), TaskName("web")) shouldBe TaskId("testing.web")
      }

      "unapplies returning the taskName" in {
        val Some((unappliedPodId, unappliedTaskName)) =
          PodTaskIdStrategy.DefaultStrategy.unapply(TaskId("testing.web"))
        unappliedPodId shouldBe PodId("testing")
        unappliedTaskName shouldBe TaskName("web")
      }
    }
  }
}
