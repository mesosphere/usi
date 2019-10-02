package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

sealed trait RunTemplateLike {
  final lazy val allResources: Seq[ResourceRequirement] = {
    val b = Seq.newBuilder[ResourceRequirement]
    taskResourceRequirements.values.foreach { r =>
      r.foreach(b += _)
    }
    executorResources.foreach { b += _ }
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
    Left(???)
  }
}

class LaunchGroupBuilder(val role: String, executorBuilder: ExecutorBuilderLike, tasks: Map[PartialTaskId, TaskBuilderLike])
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
        b.setTaskId(Mesos.TaskID.newBuilder().setValue(taskIdStrategy(partialTaskId).value))
        b.setAgentId(matchedOffer.getAgentId)
        b.addAllResources(resources.asJava)
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
  def defaultStrategy = new PodTaskIdStrategy {
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
