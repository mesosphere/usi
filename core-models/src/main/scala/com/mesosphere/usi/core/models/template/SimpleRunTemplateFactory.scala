package com.mesosphere.usi.core.models.template

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.mesosphere.usi.core.models.{TaskName, TaskBuilder}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

object SimpleRunTemplateFactory {

  case class SimpleTaskInfoBuilder(
      resourceRequirements: Seq[ResourceRequirement],
      shellCommand: String,
      role: String,
      fetch: Seq[FetchUri] = Seq.empty,
      dockerImageName: Option[String] = None)
      extends TaskBuilder {

    override def buildTask(
        matchedOffer: Mesos.Offer,
        taskResources: Seq[Mesos.Resource],
        peerTaskResources: Map[TaskName, Seq[Mesos.Resource]]): Mesos.TaskInfo.Builder = {
      val taskInfoBuilder = Mesos.TaskInfo.newBuilder()
      val uris = fetch.map { f =>
        val fetchBuilder = Mesos.CommandInfo.URI
          .newBuilder()
          .setValue(f.uri.toString)
          .setExecutable(f.executable)
          .setExtract(f.extract)
          .setCache(f.cache)
        f.outputFile.foreach { name =>
          fetchBuilder.setOutputFile(name)
        }
        fetchBuilder.build()
      }
      taskInfoBuilder.setCommand(
        Mesos.CommandInfo
          .newBuilder()
          .setShell(true)
          .setValue(shellCommand)
          .addAllUris(uris.asJava))

      dockerImageName.foreach { name =>
        taskInfoBuilder.setContainer(
          Mesos.ContainerInfo
            .newBuilder()
            .setType(Mesos.ContainerInfo.Type.MESOS)
            .setMesos(
              Mesos.ContainerInfo.MesosInfo
                .newBuilder()
                .setImage(Mesos.Image
                  .newBuilder()
                  .setType(Mesos.Image.Type.DOCKER)
                  .setDocker(Mesos.Image.Docker.newBuilder().setName(name))))
            .build())
      }
      taskInfoBuilder
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
    * @param dockerImageName      a Docker image to pull down and run of the form: [REGISTRY_HOST[:REGISTRY_PORT]/]REPOSITORY[:TAG|@DIGEST]
    */
  def apply(
      resourceRequirements: Seq[ResourceRequirement],
      shellCommand: String,
      role: String,
      fetch: Seq[FetchUri] = Seq.empty,
      dockerImageName: Option[String] = None): RunTemplate =
    new LegacyLaunchRunTemplate(
      role,
      new SimpleTaskInfoBuilder(
        resourceRequirements: Seq[ResourceRequirement],
        shellCommand,
        role,
        fetch,
        dockerImageName))
}
