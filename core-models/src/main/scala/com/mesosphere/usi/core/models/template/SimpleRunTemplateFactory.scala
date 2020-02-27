package com.mesosphere.usi.core.models.template

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.mesosphere.usi.core.models.{TaskName, TaskBuilder}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

object SimpleRunTemplateFactory {

  sealed trait Command {
    def build(): Mesos.CommandInfo.Builder
  }

  /**
    * Use `/bin/sh -c` to execute passed command.
    *
    * @param cmd The shell command that is executed.
    */
  case class Shell(cmd: String) extends Command {
    def build(): Mesos.CommandInfo.Builder = {
      Mesos.CommandInfo
        .newBuilder()
        .setShell(true)
        .setValue(cmd)
    }
  }

  /**
    * Use the entrypoint of the Docker image to execute the arguments. If the argument list is empty `CMD` from the
    * Docker image is called.
    *
    * @param args A list of arguments passed.
    */
  case class DockerEntrypoint(args: List[String]) extends Command {
    def build(): Mesos.CommandInfo.Builder = {
      val builder = Mesos.CommandInfo
        .newBuilder()
        .setShell(false)

      args.zipWithIndex.foreach { case (arg, index) => builder.setArguments(index, arg) }

      builder
    }
  }

  /**
    * This task info builder is used by the [[LegacyLaunchRunTemplate]]. I launches a simple pod.
    *
    * @param resourceRequirements The resources required for the pod.
    * @param shellCommand The command that is executed.
    * @param role The Mesos role used.
    * @param fetch The artifacts that are fetched by Mesos.
    * @param dockerImageName The optional Docker image the task will run in.
    */
  case class SimpleTaskInfoBuilder(
      resourceRequirements: Seq[ResourceRequirement],
      command: Command,
      role: String,
      fetch: Seq[FetchUri] = Seq.empty,
      dockerImageName: Option[String] = None)
      extends TaskBuilder {

    def this(
        resourceRequirements: Seq[ResourceRequirement],
        shellCommand: String,
        role: String,
        fetch: Seq[FetchUri] = Seq.empty,
        dockerImageName: Option[String] = None) =
      this(resourceRequirements, Shell(shellCommand), role, fetch, dockerImageName)

    if (command.isInstanceOf[DockerEntrypoint]) {
      assert(dockerImageName.isDefined, "The default entrypoint can only be used with a Docker image.")
    }

    override def buildTask(
        taskInfoBuilder: Mesos.TaskInfo.Builder,
        matchedOffer: Mesos.Offer,
        taskResources: Seq[Mesos.Resource],
        peerTaskResources: Map[TaskName, Seq[Mesos.Resource]]): Unit = {
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

      taskInfoBuilder.setCommand(command.build().addAllUris(uris.asJava))

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
    }
  }

  /**
    * Creates a [[LegacyLaunchRunTemplate]] that is compatible to older USI versions.
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
