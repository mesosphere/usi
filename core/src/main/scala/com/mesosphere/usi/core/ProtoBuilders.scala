package com.mesosphere.usi.core

import com.google.protobuf.ByteString
import org.apache.mesos.v1.{Protos => Mesos}

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
        allocationInfo: Mesos.Resource.AllocationInfo = null,
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

      attributes.foreach(b.addAttributes)
      executorIds.foreach(b.addExecutorIds)
      resources.foreach(b.addResources)

      if (allocationInfo != null) b.setAllocationInfo(allocationInfo)
      if (domain != null) b.setDomain(domain)
      if (frameworkID != null) b.setFrameworkId(frameworkID)
      if (unavailability != null) b.setUnavailability(unavailability)
      if (url != null) b.setUrl(url)

      b.build()
    }

    def newResource(
        name: String,
        resourceType: Mesos.Value.Type,
        reservations: Iterable[Mesos.Resource.ReservationInfo] = Nil,
        allocationInfo: Mesos.Resource.AllocationInfo = null,
        disk: Mesos.Resource.DiskInfo = null,
        providerId: Mesos.ResourceProviderID = null,
        ranges: Mesos.Value.Ranges = null,
        revocable: Mesos.Resource.RevocableInfo = null,
        scalar: Mesos.Value.Scalar = null,
        set: Mesos.Value.Set = null,
        shared: Mesos.Resource.SharedInfo = null): Mesos.Resource = {
      val b = Mesos.Resource.newBuilder()

      b.setType(resourceType)
      b.setName(name)

      reservations.foreach(b.addReservations)

      if (allocationInfo != null) b.setAllocationInfo(allocationInfo)
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
        agentId: Mesos.AgentID = null,
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
        unreachableTime: Mesos.TimeInfo = null,
        uuid: ByteString = null): Mesos.TaskStatus = {

      val b = Mesos.TaskStatus
        .newBuilder()
        .setTaskId(taskId)
        .setState(state)

      if (agentId != null) b.setAgentId(agentId)
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
        container: Mesos.ContainerInfo = null,
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
      if (container != null) b.setContainer(container)
      if (data != null) b.setData(data)
      if (discovery != null) b.setDiscovery(discovery)
      if (executor != null) b.setExecutor(executor)
      if (healthCheck != null) b.setHealthCheck(healthCheck)
      if (killPolicy != null) b.setKillPolicy(killPolicy)
      if (labels != null) b.setLabels(labels)
      if (maxCompletionTime != null) b.setMaxCompletionTime(maxCompletionTime)
      b.build()
    }
  }
