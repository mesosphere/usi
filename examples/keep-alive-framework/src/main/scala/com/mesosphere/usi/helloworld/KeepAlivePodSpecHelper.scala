package com.mesosphere.usi.helloworld

import java.util.UUID

import com.mesosphere.usi.core.models.{Goal, PodId, PodSpec, RunSpec, SpecsSnapshot}
import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}

object KeepAlivePodSpecHelper {

  val runSpec: RunSpec = RunSpec(
    resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 0.001), ScalarRequirement(ResourceType.MEM, 32)),
    shellCommand = """echo "Hello, world" && sleep 5""",
    role = "test"
  )

  def generatePodSpec(): PodSpec = {
    val podId = PodId(s"hello-world.${UUID.randomUUID()}.1")

    val podSpec = PodSpec(
      id = podId,
      goal = Goal.Running,
      runSpec = runSpec
    )

    podSpec
  }

  def specsSnapshot(numberOfPods: Int): SpecsSnapshot = SpecsSnapshot(
    podSpecs = (1 to numberOfPods).map(_ => generatePodSpec()),
    reservationSpecs = Seq.empty
  )

  def createNewIncarnationId(podId: PodId): PodId = {
    val idAndIncarnation = """^(.+\..*)\.(\d+)$""".r
    val (podIdWithoutIncarnation, currentIncarnation) = podId.value match {
      case idAndIncarnation(id, inc) =>
        id -> inc.toLong
    }
    PodId(s"$podIdWithoutIncarnation.${currentIncarnation + 1}")
  }

}
