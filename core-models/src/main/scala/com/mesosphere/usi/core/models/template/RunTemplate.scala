package com.mesosphere.usi.core.models.template

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.mesosphere.usi.core.models.{CurriedPodTaskIdStrategy, PartialTaskId, TaskBuilder}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

sealed trait RunTemplate {
  final lazy val allResourceRequirements: List[RunTemplate.KeyedResourceRequirement] = {
    val b = List.newBuilder[RunTemplate.KeyedResourceRequirement]
    taskResourceRequirements.foreach {
      case (taskId, requirements) =>
        requirements.foreach { r =>
          b += RunTemplate.KeyedResourceRequirement(Some(taskId), r)
        }
    }
    executorResources.foreach { r =>
      b += RunTemplate.KeyedResourceRequirement(None, r)
    }
    b.result()
  }
  val taskResourceRequirements: Map[PartialTaskId, Seq[ResourceRequirement]]
  val executorResources: Seq[ResourceRequirement]
  val role: String
  private[usi] def buildOperation(
      matchedOffer: Mesos.Offer,
      taskIdStrategy: CurriedPodTaskIdStrategy,
      executorResources: Seq[Mesos.Resource],
      taskResources: Map[PartialTaskId, Seq[Mesos.Resource]])
    : Either[Mesos.Offer.Operation.Launch, Mesos.Offer.Operation.LaunchGroup]
}

object RunTemplate extends StrictLogging {
  case class KeyedResourceRequirement(entityKey: Option[PartialTaskId], requirement: ResourceRequirement)

  private[models] def setTaskInfo(
      b: Mesos.TaskInfo.Builder,
      matchedOffer: Mesos.Offer,
      partialTaskId: PartialTaskId,
      taskIdStrategy: CurriedPodTaskIdStrategy,
      resources: Seq[Mesos.Resource]): Unit = {
    if (b.hasTaskId) {
      logger.error(
        s"TaskInfo builder for ${taskIdStrategy.podId} / ${partialTaskId} set the task ID but shouldn't! Value is ignored")
    }
    if (b.hasAgentId) {
      logger.error(
        s"TaskInfo builder for ${taskIdStrategy.podId} / ${partialTaskId} set the agentId but shouldn't! Value is ignored")
    }
    if (b.getResourcesCount != 0) {
      logger.error(s"TaskInfo builder for ${taskIdStrategy.podId} set resources but shouldn't! Value is ignored")
      b.clearResources()
    }
    val taskId = taskIdStrategy(partialTaskId).value
    if (!b.hasName) {
      b.setName(taskId)
    }
    b.setTaskId(Mesos.TaskID.newBuilder().setValue(taskId))
    b.setAgentId(matchedOffer.getAgentId)
    b.addAllResources(resources.asJava)
  }
}

class LaunchGroupBuilder(val role: String, executorBuilder: ExecutorBuilder, tasks: Map[PartialTaskId, TaskBuilder])
    extends RunTemplate
    with StrictLogging {
  val taskResourceRequirements = tasks.map { case (taskId, taskBuilder) => taskId -> taskBuilder.resourceRequirements }

  override val executorResources: Seq[ResourceRequirement] = Nil

  override private[usi] def buildOperation(
      matchedOffer: Mesos.Offer,
      taskIdStrategy: CurriedPodTaskIdStrategy,
      executorResources: Seq[Mesos.Resource],
      taskResources: Map[PartialTaskId, Seq[Mesos.Resource]]
  ): Either[Mesos.Offer.Operation.Launch, Mesos.Offer.Operation.LaunchGroup] = {
    val executorProtoBuilder = executorBuilder.buildExecutor(matchedOffer, executorResources)
    if (executorProtoBuilder.getResourcesCount != 0) {
      logger.error(s"Executor builder for ${taskIdStrategy.podId} set resources but shouldn't! Value is ignored")
      executorProtoBuilder.clearResources()
    }
    executorProtoBuilder.addAllResources(executorResources.asJava)

    val taskInfos = taskResources.map {
      case (partialTaskId, resources) =>
        val b = tasks(partialTaskId).buildTask(matchedOffer, resources, taskResources)
        RunTemplate.setTaskInfo(b, matchedOffer, partialTaskId, taskIdStrategy, resources)
        partialTaskId -> b.build
    }

    val tgBuilder = Mesos.TaskGroupInfo
      .newBuilder()
      .addAllTasks(taskInfos.values.asJava)

    val launchGroup = Mesos.Offer.Operation.LaunchGroup
      .newBuilder()
      .setExecutor(executorProtoBuilder)
      .setTaskGroup(tgBuilder)
      .build()

    Right(launchGroup)
  }
}

class LegacyLaunchBuilder(val role: String, taskBuilder: TaskBuilder) extends RunTemplate {
  final override val taskResourceRequirements: Map[PartialTaskId, Seq[ResourceRequirement]] =
    Map(PartialTaskId.empty -> taskBuilder.resourceRequirements)

  final override val executorResources: Seq[ResourceRequirement] = Nil

  override private[usi] def buildOperation(
      offer: Mesos.Offer,
      taskIdStrategy: CurriedPodTaskIdStrategy,
      executorResources: Seq[Mesos.Resource],
      taskResources: Map[PartialTaskId, Seq[Mesos.Resource]]
  ): Either[Mesos.Offer.Operation.Launch, Mesos.Offer.Operation.LaunchGroup] = {
    val resources = taskResources(PartialTaskId.empty)
    val taskInfoBuilder = taskBuilder
      .buildTask(offer, taskResources = resources, peerTaskResources = taskResources)
    RunTemplate.setTaskInfo(taskInfoBuilder, offer, PartialTaskId.empty, taskIdStrategy, resources)
    val op = Mesos.Offer.Operation.Launch
      .newBuilder()
      .addTaskInfos(taskInfoBuilder)
      .build
    Left(op)
  }
}
