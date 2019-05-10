package com.mesosphere.usi.helloworld

import java.util.UUID

import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.models.{PodId, RunningPodSpec, RunSpec}

/**
  * This is a helper object that generates pod specs and snapshots.
  */
object KeepAlivePodSpecHelper {

  val runSpec: RunSpec = RunSpec(
    resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 0.001), ScalarRequirement(ResourceType.MEM, 32)),
    shellCommand = """echo "Hello, world" && sleep 30""",
    role = "test"
  )

  def generatePodSpec(): RunningPodSpec = {
    val podId = PodId(s"hello-world.${UUID.randomUUID()}.1")

    val podSpec = RunningPodSpec(id = podId, runSpec = runSpec)
    podSpec
  }

  def specsSnapshot(numberOfPods: Int): List[RunningPodSpec] =
    (1 to numberOfPods).map(_ => generatePodSpec())(collection.breakOut)

  def createNewIncarnationId(podId: PodId): PodId = {
    val idAndIncarnation = """^(.+\..*)\.(\d+)$""".r
    val (podIdWithoutIncarnation, currentIncarnation) = podId.value match {
      case idAndIncarnation(id, inc) =>
        id -> inc.toLong
    }
    PodId(s"$podIdWithoutIncarnation.${currentIncarnation + 1}")
  }

}
