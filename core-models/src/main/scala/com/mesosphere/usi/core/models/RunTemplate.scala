package com.mesosphere.usi.core.models

import com.google.protobuf.ByteString
import com.mesosphere.usi.core.models.resources._
import org.apache.mesos.v1.Protos.ExecutorInfo
import org.apache.mesos.v1.Protos.Value.Scalar
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

/**
  * We have basically two different types of RunTemplates:
  *   - TaskRunTemplate provides a [[Mesos.TaskInfo.Builder]]
  *   - TaskGroupRunTemplate provides a [[Mesos.TaskGroupInfo.Builder]] and optionally a [[Mesos.ExecutorInfo]]
  *
  * For the TaskRunTemplate, there's another abstraction, [[SimpleRunTemplate]] which doesn't bother with Protos, but
  * allows a user to define a very simple task
  */
sealed trait RunTemplate {
  def resourceRequirements: Seq[ResourceRequirement]
  def role: String
}

sealed trait TaskRunTemplateType extends RunTemplate {
  def protoBuilder: Mesos.TaskInfo.Builder
}

sealed trait TaskGroupRunTemplateType extends RunTemplate {
  def protoBuilder: Mesos.TaskGroupInfo.Builder
  def executorBuilder: Mesos.ExecutorInfo.Builder
}

case class TaskRunTemplate(task: Mesos.TaskInfo.Builder, role: String) extends TaskRunTemplateType {
  override def resourceRequirements: Seq[ResourceRequirement] =
    RunTemplate.mapMesosResourceRequirements(task.getResourcesList.iterator().asScala.toSeq)

  override def protoBuilder: Mesos.TaskInfo.Builder = task
}

case class TaskGroupRunTemplate(taskGroup: Mesos.TaskGroupInfo.Builder, executor: Option[Mesos.ExecutorInfo.Builder] = None, role: String) extends TaskGroupRunTemplateType {
  override def resourceRequirements: Seq[ResourceRequirement] = {
    val taskList = taskGroup.getTasksList.iterator().asScala.toSeq

    // No need to merge the duplicate types of resources, the OfferMatcher still needs to fulfill everything,
    // even if it's split up
    taskList.flatMap(task => RunTemplate.mapMesosResourceRequirements(task.getResourcesList.iterator().asScala.toSeq))
  }

  override def protoBuilder: Mesos.TaskGroupInfo.Builder = taskGroup

  override def executorBuilder: ExecutorInfo.Builder = executor.getOrElse(RunTemplate.buildDefaultExecutor())
}

/**
  * Launch template used to launch a pod
  *
  * Support is rather primitive now. We will add richer support for TaskInfo and TaskGroupInfo specification in
  * (DCOS-48503, DCOS-47481)
  *
  * @param resourceRequirements a list of resource requirements for the [[RunningPodSpec]]. See [[ResourceRequirement]]
  *                             class for more information
  * @param shellCommand         tasks' shell command
  * @param fetch                a list of artifact URIs that are passed to Mesos fetcher module and resolved at runtime
  */
case class SimpleRunTemplate(
    resourceRequirements: Seq[ResourceRequirement],
    shellCommand: String,
    role: String,
    fetch: Seq[FetchUri] = Seq.empty)
    extends TaskRunTemplateType {

  override def protoBuilder: Mesos.TaskInfo.Builder = {
    // Note - right now, runSpec only describes a single task. This needs to be improved in the future.
    val taskInfo = newTaskInfo(
      command = newCommandInfo(shellCommand, fetch)
    )
    taskInfo
  }

  def newTaskInfo(
      command: Mesos.CommandInfo,
      check: Mesos.CheckInfo = null,
      container: Mesos.ContainerInfo = null,
      data: ByteString = null,
      discovery: Mesos.DiscoveryInfo = null,
      executor: Mesos.ExecutorInfo = null,
      healthCheck: Mesos.HealthCheck = null,
      killPolicy: Mesos.KillPolicy = null,
      labels: Mesos.Labels = null,
      maxCompletionTime: Mesos.DurationInfo = null): Mesos.TaskInfo.Builder = {

    val b = Mesos.TaskInfo
      .newBuilder()
      .setCommand(command)

    if (check != null) b.setCheck(check)
    if (container != null) b.setContainer(container)
    if (data != null) b.setData(data)
    if (discovery != null) b.setDiscovery(discovery)
    if (executor != null) b.setExecutor(executor)
    if (healthCheck != null) b.setHealthCheck(healthCheck)
    if (killPolicy != null) b.setKillPolicy(killPolicy)
    if (labels != null) b.setLabels(labels)
    if (maxCompletionTime != null) b.setMaxCompletionTime(maxCompletionTime)

    b
  }

  def newCommandInfo(shellCommand: String, fetchUri: Seq[FetchUri]): Mesos.CommandInfo = {
    val b = Mesos.CommandInfo
      .newBuilder()
      .setShell(true)
      .setValue(shellCommand)
    fetchUri.map(u => b.addUris(newURI(u)))
    b.build()
  }

  def newURI(fetch: FetchUri): Mesos.CommandInfo.URI = {
    val b = Mesos.CommandInfo.URI
      .newBuilder()
      .setValue(fetch.uri.toString)
      .setExecutable(fetch.executable)
      .setExtract(fetch.extract)
      .setCache(fetch.cache)
    fetch.outputFile.foreach { name =>
      b.setOutputFile(name)
    }
    b.build()
  }
}


object RunTemplate {

  private[models] def buildDefaultExecutor(): ExecutorInfo.Builder = {
    ExecutorInfo.newBuilder()
      .setType(ExecutorInfo.Type.DEFAULT)
      .addResources(Mesos.Resource.newBuilder().setName(ResourceType.CPUS.name).setType(Mesos.Value.Type.SCALAR).setScalar(Scalar.newBuilder().setValue(0.1)))
      .addResources(Mesos.Resource.newBuilder().setName(ResourceType.MEM.name).setType(Mesos.Value.Type.SCALAR).setScalar(Scalar.newBuilder().setValue(32)))
      .addResources(Mesos.Resource.newBuilder().setName(ResourceType.DISK.name).setType(Mesos.Value.Type.SCALAR).setScalar(Scalar.newBuilder().setValue(10)))
  }

  private[models] def mapMesosResourceRequirements(mesosResources: Seq[Mesos.Resource]): Seq[ResourceRequirement] = {
    mesosResources.map { res =>
      val resType = ResourceType.fromName(res.getName)
      res.getType match {
        case Mesos.Value.Type.SCALAR =>
          ScalarRequirement(resType, res.getScalar.getValue)
        case Mesos.Value.Type.RANGES =>
          RangeRequirement(
            res.getRanges.getRangeList
              .iterator()
              .asScala
              .toSeq
              .map(
                r => RangeValue(r.getBegin.toInt, r.getEnd.toInt)
              ),
            resType)
        case _ => ???
      }
    }
  }
}