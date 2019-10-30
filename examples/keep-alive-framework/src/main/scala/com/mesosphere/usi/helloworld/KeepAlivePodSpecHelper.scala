package com.mesosphere.usi.helloworld

import java.util.UUID

import com.mesosphere.usi.core.models.resources.{ResourceType, ScalarRequirement}
import com.mesosphere.usi.core.models.template.{RunTemplate, SimpleRunTemplateFactory}
import com.mesosphere.usi.core.models.{PodId, RunningPodSpec}
import com.typesafe.scalalogging.StrictLogging

/**
  * This is a helper object that generates pod specs and snapshots.
  */
object KeepAlivePodSpecHelper extends StrictLogging {

  val runSpec: RunTemplate = SimpleRunTemplateFactory(
    resourceRequirements = List(ScalarRequirement(ResourceType.CPUS, 0.001), ScalarRequirement(ResourceType.MEM, 32)),
    shellCommand = """echo "Hello, world" && sleep 30""",
    role = "test"
  )

  def generatePodSpec(): RunningPodSpec = {
    val podId = PodId(s"hello-world_${UUID.randomUUID()}_1")

    logger.info(s"Generating PodSpec for '${podId}'")
    val podSpec = RunningPodSpec(id = podId, runSpec = runSpec)
    podSpec
  }

  def specsSnapshot(numberOfPods: Int): List[RunningPodSpec] =
    (1 to numberOfPods).map(_ => generatePodSpec())(collection.breakOut)

  def createNewIncarnationId(podId: PodId): PodId = {
    val idAndIncarnation = """^(.+_.*)_(\d+)$""".r
    val (podIdWithoutIncarnation, currentIncarnation) = podId.value match {
      case idAndIncarnation(id, inc) =>
        id -> inc.toLong
      case _ => throw new IllegalArgumentException(s"Failed to create new incarnation id for ${podId.value}")
    }
    PodId(s"${podIdWithoutIncarnation}_${currentIncarnation + 1}")
  }

}
