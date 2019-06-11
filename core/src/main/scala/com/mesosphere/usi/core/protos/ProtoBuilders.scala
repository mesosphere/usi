package com.mesosphere.usi.core.protos
import com.google.protobuf.ByteString
import com.mesosphere.usi.core.models.FetchUri
import org.apache.mesos.v1.Protos.{ContainerInfo, Image}
import org.apache.mesos.v1.{Protos => Mesos}
import org.apache.mesos.v1.scheduler.Protos.{Event => MesosEvent}

/**
  * Collection of helper ProtoBuilders for convenience
  *
  * These are maintained by hand. All proto builders implemented should follow the following rule:
  *
  * - Repeated proto fields are specified by an Iterable, which defaults to empty
  * - Required fields are required by the method signature
  * - Non-required fields should default to null, with the exception of primitives (int, double, etc.); primitives use Optional out of necessity
  */
private[usi] object ProtoBuilders {
  def newAgentId(id: String): Mesos.AgentID = {
    Mesos.AgentID.newBuilder().setValue(id).build
  }

  def newOfferId(id: String): Mesos.OfferID = {
    Mesos.OfferID.newBuilder().setValue(id).build
  }

  def newFrameworkId(id: String): Mesos.FrameworkID = {
    Mesos.FrameworkID.newBuilder().setValue(id).build
  }

  def newOffer(
      id: Mesos.OfferID,
      agentId: Mesos.AgentID,
      frameworkID: Mesos.FrameworkID,
      hostname: String,
      allocationInfo: Mesos.Resource.AllocationInfo,
      domain: Mesos.DomainInfo = null,
      executorIds: Iterable[Mesos.ExecutorID] = Nil,
      attributes: Iterable[Mesos.Attribute] = Nil,
      resources: Iterable[Mesos.Resource] = Nil,
      unavailability: Mesos.Unavailability = null,
      url: Mesos.URL = null): Mesos.Offer = {

    val b = Mesos.Offer
      .newBuilder()
      .setId(id)
      .setAgentId(agentId)
      .setHostname(hostname)
      .setAllocationInfo(allocationInfo)

    attributes.foreach(b.addAttributes)
    executorIds.foreach(b.addExecutorIds)
    resources.foreach(b.addResources)

    if (domain != null) b.setDomain(domain)
    if (frameworkID != null) b.setFrameworkId(frameworkID)
    if (unavailability != null) b.setUnavailability(unavailability)
    if (url != null) b.setUrl(url)

    b.build()
  }

  def newResourceReservationInfo(
      reservationType: Mesos.Resource.ReservationInfo.Type,
      role: String,
      principal: String,
      labels: Mesos.Labels = null): Mesos.Resource.ReservationInfo = {
    val b = Mesos.Resource.ReservationInfo
      .newBuilder()
      .setPrincipal(principal)
      .setRole(role)
      .setType(reservationType)
    if (labels != null) b.setLabels(labels)

    b.build()
  }

  def newResourceAllocationInfo(role: String): Mesos.Resource.AllocationInfo = {
    Mesos.Resource.AllocationInfo
      .newBuilder()
      .setRole(role)
      .build()
  }

  def newResource(
      name: String,
      resourceType: Mesos.Value.Type,
      allocationInfo: Mesos.Resource.AllocationInfo,
      reservations: Iterable[Mesos.Resource.ReservationInfo] = Nil,
      disk: Mesos.Resource.DiskInfo = null,
      providerId: Mesos.ResourceProviderID = null,
      ranges: Mesos.Value.Ranges = null,
      revocable: Mesos.Resource.RevocableInfo = null,
      scalar: Mesos.Value.Scalar = null,
      set: Mesos.Value.Set = null,
      shared: Mesos.Resource.SharedInfo = null): Mesos.Resource = {

    val b = Mesos.Resource
      .newBuilder()
      .setType(resourceType)
      .setName(name)
      .setAllocationInfo(allocationInfo)

    reservations.foreach(b.addReservations)

    if (disk != null) b.setDisk(disk)
    if (providerId != null) b.setProviderId(providerId)
    if (ranges != null) b.setRanges(ranges)
    if (revocable != null) b.setRevocable(revocable)
    if (scalar != null) b.setScalar(scalar)
    if (set != null) b.setSet(set)
    if (shared != null) b.setShared(shared)

    b.build()
  }

  def newTaskId(value: String): Mesos.TaskID =
    Mesos.TaskID.newBuilder().setValue(value).build()

  def newTaskStatus(
      taskId: Mesos.TaskID,
      state: Mesos.TaskState,
      agentId: Mesos.AgentID,
      uuid: ByteString = null, // UUID should be present, only e.g. TASK_ERROR does not have UUID
      checkStatus: Mesos.CheckStatusInfo = null,
      containerStatus: Mesos.ContainerStatus = null,
      data: ByteString = null,
      executorId: Mesos.ExecutorID = null,
      healthy: Option[Boolean] = None,
      labels: Mesos.Labels = null,
      limitation: Mesos.TaskResourceLimitation = null,
      message: String = null,
      reason: Mesos.TaskStatus.Reason = null,
      source: Mesos.TaskStatus.Source = null,
      timestamp: Double = 0,
      unreachableTime: Mesos.TimeInfo = null): Mesos.TaskStatus = {

    val b = Mesos.TaskStatus
      .newBuilder()
      .setTaskId(taskId)
      .setState(state)
      .setAgentId(agentId)

    if (checkStatus != null) b.setCheckStatus(checkStatus)
    if (containerStatus != null) b.setContainerStatus(containerStatus)
    if (data != null) b.setData(data)
    if (executorId != null) b.setExecutorId(executorId)
    healthy.foreach(b.setHealthy)
    if (labels != null) b.setLabels(labels)
    if (limitation != null) b.setLimitation(limitation)
    if (message != null) b.setMessage(message)
    if (reason != null) b.setReason(reason)
    if (source != null) b.setSource(source)
    if (timestamp != 0) b.setTimestamp(timestamp)
    if (unreachableTime != null) b.setUnreachableTime(unreachableTime)
    if (uuid != null) b.setUuid(uuid)
    b.build()
  }

  def newOperationId(value: String): Mesos.OperationID =
    Mesos.OperationID.newBuilder().setValue(value).build()

  def newOfferOperation(
      operationType: Mesos.Offer.Operation.Type,
      id: Mesos.OperationID = null,
      create: Mesos.Offer.Operation.Create = null,
      createDisk: Mesos.Offer.Operation.CreateDisk = null,
      destroy: Mesos.Offer.Operation.Destroy = null,
      destroyDisk: Mesos.Offer.Operation.DestroyDisk = null,
      growVolume: Mesos.Offer.Operation.GrowVolume = null,
      launch: Mesos.Offer.Operation.Launch = null,
      launchGroup: Mesos.Offer.Operation.LaunchGroup = null,
      reserve: Mesos.Offer.Operation.Reserve = null,
      shrinkVolume: Mesos.Offer.Operation.ShrinkVolume = null,
      unreserve: Mesos.Offer.Operation.Unreserve = null): Mesos.Offer.Operation = {

    val b = Mesos.Offer.Operation
      .newBuilder()
      .setType(operationType)

    if (id != null) b.setId(id)
    if (create != null) b.setCreate(create)
    if (createDisk != null) b.setCreateDisk(createDisk)
    if (destroy != null) b.setDestroy(destroy)
    if (destroyDisk != null) b.setDestroyDisk(destroyDisk)
    if (growVolume != null) b.setGrowVolume(growVolume)
    if (launch != null) b.setLaunch(launch)
    if (launchGroup != null) b.setLaunchGroup(launchGroup)
    if (reserve != null) b.setReserve(reserve)
    if (shrinkVolume != null) b.setShrinkVolume(shrinkVolume)
    if (unreserve != null) b.setUnreserve(unreserve)
    b.build()
  }

  def newOfferOperationLaunch(taskInfos: Iterable[Mesos.TaskInfo]): Mesos.Offer.Operation.Launch = {
    val b = Mesos.Offer.Operation.Launch.newBuilder()
    taskInfos.foreach(b.addTaskInfos)
    b.build()
  }

  def newTaskInfo(
      taskId: Mesos.TaskID,
      name: String,
      agentId: Mesos.AgentID,
      command: Mesos.CommandInfo,
      check: Mesos.CheckInfo = null,
      container: Option[Mesos.ContainerInfo],
      data: ByteString = null,
      discovery: Mesos.DiscoveryInfo = null,
      executor: Mesos.ExecutorInfo = null,
      healthCheck: Mesos.HealthCheck = null,
      killPolicy: Mesos.KillPolicy = null,
      labels: Mesos.Labels = null,
      maxCompletionTime: Mesos.DurationInfo = null,
      resources: Iterable[Mesos.Resource] = Nil): Mesos.TaskInfo = {

    val b = Mesos.TaskInfo
      .newBuilder()
      .setTaskId(taskId)
      .setName(name)
      .setAgentId(agentId)
      .setCommand(command)

    resources.foreach(b.addResources)
    if (check != null) b.setCheck(check)
    container.foreach(b.setContainer(_))
    if (data != null) b.setData(data)
    if (discovery != null) b.setDiscovery(discovery)
    if (executor != null) b.setExecutor(executor)
    if (healthCheck != null) b.setHealthCheck(healthCheck)
    if (killPolicy != null) b.setKillPolicy(killPolicy)
    if (labels != null) b.setLabels(labels)
    if (maxCompletionTime != null) b.setMaxCompletionTime(maxCompletionTime)
    b.build()
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

  def newValueRanges(ranges: Iterable[Mesos.Value.Range]): Mesos.Value.Ranges = {
    val b = Mesos.Value.Ranges.newBuilder()
    ranges.foreach(b.addRange)
    b.build()
  }

  def newTaskUpdateEvent(taskStatus: Mesos.TaskStatus): MesosEvent = {
    MesosEvent
      .newBuilder()
      .setUpdate(MesosEvent.Update.newBuilder().setStatus(taskStatus))
      .build()
  }

  def newOfferEvent(offer: Mesos.Offer): MesosEvent = {
    MesosEvent
      .newBuilder()
      .setOffers(MesosEvent.Offers.newBuilder().addOffers(offer))
      .build()
  }

  def newDockerImage(dockerImageName: String): Image = {
    Image
      .newBuilder()
      .setType(Image.Type.DOCKER)
      .setDocker(Image.Docker.newBuilder().setName(dockerImageName).build())
      .build()
  }

  def newContainerInfo(imageName: Option[String]): Option[Mesos.ContainerInfo] = {
    imageName.map { name =>
      ContainerInfo
        .newBuilder()
        .setType(Mesos.ContainerInfo.Type.MESOS)
        .setMesos(ContainerInfo.MesosInfo.newBuilder().setImage(newDockerImage(name)).build())
        .build()
    }
  }
}
