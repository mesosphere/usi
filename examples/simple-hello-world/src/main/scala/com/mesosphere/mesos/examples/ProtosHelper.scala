package com.mesosphere.mesos.examples
import java.util.UUID

import org.apache.mesos.v1.Protos.Offer.Operation
import org.apache.mesos.v1.Protos.Offer.Operation.Launch
import org.apache.mesos.v1.Protos.Value.Scalar
import org.apache.mesos.v1.Protos.{
  AgentID,
  CommandInfo,
  FrameworkID,
  FrameworkInfo,
  Offer,
  Resource,
  TaskID,
  TaskInfo,
  Value
}
import org.apache.mesos.v1.scheduler.Protos.Call.Accept

import scala.collection.JavaConverters._
import scala.sys.SystemProperties

/**
  * A helper object that simplifies building Mesos protobuf message.
  */
object ProtosHelper {

  /**
    * Return a framework info with certain parameters set. Some others like roles and capabilities are hardcoded since
    * different ones are not supported in this simple example.
    *
    * @param user framework user
    * @param name framework name
    * @param frameworkId framework Id
    * @return a [[FrameworkInfo.Builder]]
    */
  def frameworkInfo(
      user: String,
      name: String,
      frameworkId: String = UUID.randomUUID().toString): FrameworkInfo.Builder = {
    FrameworkInfo
      .newBuilder()
      .setUser(
        new SystemProperties()
          .get("user.name")
          .getOrElse(throw new IllegalArgumentException("A local user is needed to launch Mesos tasks")))
      .setName("RawHelloWorldExample")
      .setId(FrameworkID.newBuilder.setValue(frameworkId))
      .addRoles("test")
      .addCapabilities(FrameworkInfo.Capability
        .newBuilder()
        .setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
      .setFailoverTimeout(0d)
  }

  /**
    * Returns a scalar resource builder for a resource with a give name and value.
    *
    * @param name resource name
    * @param value amount of resource
    * @return a [[Resource.Builder]]
    */
  def scalarResource(name: String, value: Double): Resource.Builder = {
    Resource
      .newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(
        Scalar
          .newBuilder()
          .setValue(value))
  }

  /**
    * A task info builder. Currently only tasks with a shell command are supported
    *
    * @param task task object
    * @param agentID agent Id
    * @return a [[TaskInfo.Builder]]
    */
  def taskInfo(task: Task, agentID: AgentID): TaskInfo.Builder = {
    TaskInfo
      .newBuilder()
      .setName(task.spec.name)
      .setTaskId(TaskID.newBuilder().setValue(task.taskId))
      .setAgentId(agentID)
      .addAllResources(
        Seq(
          scalarResource("cpus", task.spec.cpus).build(),
          scalarResource("mem", task.spec.mem).build()
        ).asJava)
      .setCommand(
        CommandInfo
          .newBuilder()
          .setShell(true)
          .setValue(task.spec.cmd))
  }

  /**
    * An offer accept builder.
    *
    * @param task task object
    * @param offer matched offer
    * @return a [[Accept]] builder
    */
  def accept(task: Task, offer: Offer): Accept.Builder = {
    Accept
      .newBuilder()
      .addAllOfferIds(Seq(offer.getId).asJava)
      .addOperations(
        Operation.newBuilder
          .setType(Operation.Type.LAUNCH)
          .setLaunch(Launch
            .newBuilder()
            .addTaskInfos(taskInfo(task, offer.getAgentId))))
  }
}
