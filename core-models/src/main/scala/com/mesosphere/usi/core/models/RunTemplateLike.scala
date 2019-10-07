package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

sealed trait RunTemplateLike {
  final lazy val allResourceRequirements: List[RunTemplateLike.KeyedResourceRequirement] = {
    val b = List.newBuilder[RunTemplateLike.KeyedResourceRequirement]
    taskResourceRequirements.foreach {
      case (taskId, requirements) =>
        requirements.foreach { r =>
          b += RunTemplateLike.KeyedResourceRequirement(Some(taskId), r)
        }
    }
    executorResources.foreach { r =>
      b += RunTemplateLike.KeyedResourceRequirement(None, r)
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

object RunTemplateLike extends StrictLogging {
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

class LegacyLaunchBuilder(val role: String, taskBuilder: TaskBuilderLike) extends RunTemplateLike {
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
    RunTemplateLike.setTaskInfo(taskInfoBuilder, offer, PartialTaskId.empty, taskIdStrategy, resources)
    val op = Mesos.Offer.Operation.Launch
      .newBuilder()
      .addTaskInfos(taskInfoBuilder)
      .build
    Left(op)
  }
}

class LaunchGroupBuilder(
    val role: String,
    executorBuilder: ExecutorBuilderLike,
    tasks: Map[PartialTaskId, TaskBuilderLike])
    extends RunTemplateLike
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
        RunTemplateLike.setTaskInfo(b, matchedOffer, partialTaskId, taskIdStrategy, resources)
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

abstract class PodTaskIdStrategy {
  def apply(podId: PodId, partialTaskId: PartialTaskId): TaskId
  def unapply(taskId: TaskId): Option[(PodId, PartialTaskId)]
  final def curried(podId: PodId): CurriedPodTaskIdStrategy = CurriedPodTaskIdStrategy(podId, this)
}

object PodTaskIdStrategy {
  def defaultStrategy: PodTaskIdStrategy = new PodTaskIdStrategy {
    override def apply(podId: PodId, partialTaskId: PartialTaskId): TaskId =
      TaskId(podId.value + "." + partialTaskId)

    override def unapply(taskId: TaskId): Option[(PodId, PartialTaskId)] =
      taskId.value.split("\\.") match {
        case Array(podId, partialTaskId) => Some((PodId(podId), PartialTaskId(partialTaskId)))
        case _ => None
      }
  }
}

case class CurriedPodTaskIdStrategy(podId: PodId, strategy: PodTaskIdStrategy) {
  def apply(partialTaskId: PartialTaskId): TaskId = strategy.apply(podId, partialTaskId)
}
object CurriedPodTaskIdStrategy {
  def default(podId: PodId): CurriedPodTaskIdStrategy =
    CurriedPodTaskIdStrategy(podId, PodTaskIdStrategy.defaultStrategy)
}

trait ExecutorBuilderLike {
  def resourceRequirements: Seq[ResourceRequirement]
  def buildExecutor(matchedOffer: Mesos.Offer, executorResources: Seq[Mesos.Resource]): Mesos.ExecutorInfo.Builder
}

trait TaskBuilderLike {
  def resourceRequirements: Seq[ResourceRequirement]
  def buildTask(
      matchedOffer: Mesos.Offer,
      taskResources: Seq[Mesos.Resource],
      peerTaskResources: Map[PartialTaskId, Seq[Mesos.Resource]]): Mesos.TaskInfo.Builder
}
