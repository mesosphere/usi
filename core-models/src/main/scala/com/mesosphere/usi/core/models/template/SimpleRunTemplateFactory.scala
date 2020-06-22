package com.mesosphere.usi.core.models.template

import java.util.Optional

import com.mesosphere.usi.core.models.resources.ResourceRequirement
import com.mesosphere.usi.core.models.{TaskBuilder, TaskName}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.varargs
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
  class DockerEntrypoint private (args: Seq[String]) extends Command {
    def build(): Mesos.CommandInfo.Builder = {
      val builder = Mesos.CommandInfo
        .newBuilder()
        .setShell(false)

      if (args.nonEmpty) builder.addAllArguments(args.asJava)

      builder
    }
  }

  object DockerEntrypoint {

    /** Factory method for [[com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory.DockerEntrypoint]]; Scala Api. */
    def apply(args: Seq[String]) = new DockerEntrypoint(args)

    /** Factory method for [[com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory.DockerEntrypoint]]; Java Api. */
    @varargs
    def create(args: String*) = new DockerEntrypoint(args)
  }

  /**
    * This task info builder is used by the [[LegacyLaunchRunTemplate]]. I launches a simple pod.
    *
    * @param resourceRequirements The resources required for the pod.
    * @param command The command that is executed.
    * @param fetch The artifacts that are fetched by Mesos.
    * @param dockerImageName The optional Docker image the task will run in.
    */
  class SimpleTaskInfoBuilder private (
      val resourceRequirements: Seq[ResourceRequirement],
      command: Command,
      fetch: Seq[FetchUri],
      dockerImageName: Option[String]
  ) extends TaskBuilder {

    if (command.isInstanceOf[DockerEntrypoint]) {
      assert(dockerImageName.isDefined, "The default entrypoint can only be used with a Docker image.")
    }

    override def buildTask(
        taskInfoBuilder: Mesos.TaskInfo.Builder,
        matchedOffer: Mesos.Offer,
        taskResources: Seq[Mesos.Resource],
        peerTaskResources: Map[TaskName, Seq[Mesos.Resource]]
    ): Unit = {
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
                .setImage(
                  Mesos.Image
                    .newBuilder()
                    .setType(Mesos.Image.Type.DOCKER)
                    .setDocker(Mesos.Image.Docker.newBuilder().setName(name))
                )
            )
            .build()
        )
      }
    }
  }

  object SimpleTaskInfoBuilder {

    def apply(
        resourceRequirements: Seq[ResourceRequirement],
        command: Command,
        fetch: Seq[FetchUri],
        dockerImageName: Option[String]
    ): SimpleTaskInfoBuilder = {
      new SimpleTaskInfoBuilder(resourceRequirements, command, fetch, dockerImageName)
    }

    def apply(
        resourceRequirements: Seq[ResourceRequirement],
        shellCommand: String,
        fetch: Seq[FetchUri] = Seq.empty,
        dockerImageName: Option[String] = None
    ): SimpleTaskInfoBuilder =
      apply(resourceRequirements, Shell(shellCommand), fetch, dockerImageName)

    /** Factory method for [[com.mesosphere.usi.core.models.template.SimpleRunTemplateFactory.SimpleTaskInfoBuilder]]; Java Api. */
    def create(
        resourceRequirements: java.util.List[ResourceRequirement],
        command: Command,
        fetch: java.util.List[FetchUri],
        dockerImageName: Optional[String]
    ): SimpleTaskInfoBuilder = {
      val image = if (dockerImageName.isPresent) Some(dockerImageName.get()) else None
      apply(resourceRequirements.asScala.toSeq, command, fetch.asScala.toSeq, image)
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
      dockerImageName: Option[String] = None
  ): RunTemplate =
    new LegacyLaunchRunTemplate(
      role,
      SimpleTaskInfoBuilder(resourceRequirements: Seq[ResourceRequirement], shellCommand, fetch, dockerImageName)
    )
}
