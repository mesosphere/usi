package com.mesosphere.usi.core.helpers

import com.mesosphere.usi.core.matching.ScalarResourceRequirement
import com.mesosphere.usi.core.models.Goal
import com.mesosphere.usi.core.models.PodId
import com.mesosphere.usi.core.models.PodSpec
import com.mesosphere.usi.core.models.ResourceType
import com.mesosphere.usi.core.models.RunSpec

object FrameworkMock {

  val mockPodId: PodId = PodId("mocked-podId")
  val mockRunSpec: RunSpec = RunSpec(
    List(ScalarResourceRequirement(ResourceType.CPUS, 1), ScalarResourceRequirement(ResourceType.MEM, 256)),
    shellCommand = "sleep 3600")

  val mockPodSpec: PodSpec = PodSpec(
    mockPodId,
    Goal.Running,
    mockRunSpec
  )

}
