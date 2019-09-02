package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.RunTemplate.mapMesosResourceRequirements
import com.mesosphere.usi.core.models.resources._
import org.apache.mesos.v1.Protos.{Resource, TaskGroupInfo, TaskInfo, Value}

import scala.collection.JavaConverters._

sealed trait RunTemplate {
  def resourceRequirements: Seq[ResourceRequirement]
  def role: String
}

object RunTemplate {
  private[models] def mapMesosResourceRequirements(mesosResources: Seq[Resource]): Seq[ResourceRequirement] = {
    mesosResources.map { res =>
      val resType = ResourceType.fromName(res.getName)
      res.getType match {
        case Value.Type.SCALAR =>
          ScalarRequirement(resType, res.getScalar.getValue)
        case Value.Type.RANGES =>
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
    extends RunTemplate

case class TaskRunTemplate(task: TaskInfo, role: String) extends RunTemplate {
  override def resourceRequirements: Seq[ResourceRequirement] =
    mapMesosResourceRequirements(task.getResourcesList.iterator().asScala.toSeq)
}

case class TaskGroupRunTemplate(taskGroup: TaskGroupInfo, role: String) extends RunTemplate {
  override def resourceRequirements: Seq[ResourceRequirement] = {
    val taskList = taskGroup.getTasksList.iterator().asScala.toSeq
    // TODO: Merge requested resources correctly?
    taskList.flatMap(task => mapMesosResourceRequirements(task.getResourcesList.iterator().asScala.toSeq))
  }
}
