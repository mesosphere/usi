package com.mesosphere.usi.core.models

import com.mesosphere.utils.UnitTest

class PodTaskIdStrategyTest extends UnitTest {
  "default strategy" should {
    "when dealing with PartialTaskId.empty" should {
      "returns a TaskId with the podId verbatim" in {
        PodTaskIdStrategy.defaultStrategy.apply(PodId("testing"), PartialTaskId.empty) shouldBe TaskId("testing")
      }

      "unapplies returning an empty taskId" in {
        val Some((unappliedPodId, unappliedPartialTaskId)) =
          PodTaskIdStrategy.defaultStrategy.unapply(TaskId("testing"))
        unappliedPodId shouldBe PodId("testing")
        unappliedPartialTaskId shouldBe PartialTaskId.empty
      }
    }
  }
}
