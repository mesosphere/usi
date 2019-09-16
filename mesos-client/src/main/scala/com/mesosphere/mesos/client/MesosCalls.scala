package com.mesosphere.mesos.client

import akka.util.ByteString
import com.google.protobuf
import org.apache.mesos.v1.Protos.{AgentID, ExecutorID, Filters, FrameworkID, KillPolicy, OfferID, Request, TaskID}
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.{Accept, Decline, Reconcile, Revive}

class MesosCalls(frameworkId: FrameworkID) {

  /**
    * ***************************************************************************
    * Helper methods to create mesos `Call`s
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#calls
    * ***************************************************************************
    */
  /**
    * Factory method to construct a TEARDOWN Mesos Call event. Calling this method has no side effects.
    *
    * This event is sent by the scheduler when it wants to tear itself down. When Mesos receives this request it will
    * shut down all executors (and consequently kill tasks). It then removes the framework and closes all open
    * connections from this scheduler to the Master.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#teardown
    */
  def newTeardown(): Call = {
    Call
      .newBuilder()
      .setType(Call.Type.TEARDOWN)
      .setFrameworkId(frameworkId)
      .build()
  }

  /**
    * Factory method to construct a ACCEPT Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler when it accepts offer(s) sent by the master. The ACCEPT request includes the type
    * of operations (e.g., launch task, launch task group, reserve resources, create volumes) that the scheduler
    * wants to perform on the offers. Note that until the scheduler replies (accepts or declines) to an offer,
    * the offer's resources are considered allocated to the offer's role and to the framework.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#accept
    */
  def newAccept(accepts: Accept): Call = {
    Call
      .newBuilder()
      .setType(Call.Type.ACCEPT)
      .setFrameworkId(frameworkId)
      .setAccept(accepts)
      .build()
  }

  /**
    * Factory method to construct a DECLINE Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to explicitly decline offer(s) received. Note that this is same as sending an ACCEPT
    * call with no operations.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#decline
    */
  def newDecline(offerIds: Seq[OfferID], filters: Option[Filters] = None): Call = {
    val declineBuilder = Decline.newBuilder()
    offerIds.foreach(declineBuilder.addOfferIds)
    filters.foreach(declineBuilder.setFilters)

    Call
      .newBuilder()
      .setType(Call.Type.DECLINE)
      .setFrameworkId(frameworkId)
      .setDecline(declineBuilder.build())
      .build()
  }

  /**
    * Factory method to construct a REVIVE Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to remove any/all filters that it has previously set via ACCEPT or DECLINE calls.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#revive
    */
  def newRevive(role: Option[String] = None): Call = {
    val reviveBuilder = Revive.newBuilder()
    role.foreach(reviveBuilder.addRoles)

    Call
      .newBuilder()
      .setType(Call.Type.REVIVE)
      .setFrameworkId(frameworkId)
      .setRevive(reviveBuilder.build())
      .build()
  }

  /**
    * Factory method to construct a SUPPRESS Mesos Call event. Calling this method has no side effects.
    *
    * Suppress offers for the specified role. If `role` is empty the `SUPPRESS` call will suppress offers for all
    * of the role the framework is currently subscribed to.
    *
    * http://mesos.apache.org/documentation/latest/upgrades/#1-2-x-revive-suppress
    */
  def newSuppress(role: Option[String] = None): Call = {
    val suppressBuilder = Call.Suppress.newBuilder()
    role.foreach(suppressBuilder.addRoles)

    Call
      .newBuilder()
      .setType(Call.Type.SUPPRESS)
      .setFrameworkId(frameworkId)
      .setSuppress(suppressBuilder)
      .build()
  }

  /**
    * Factory method to construct a KILL Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to kill a specific task. If the scheduler has a custom executor, the kill is forwarded
    * to the executor; it is up to the executor to kill the task and send a TASK_KILLED (or TASK_FAILED) update.
    * If the task hasn't yet been delivered to the executor when Mesos master or agent receives the kill request,
    * a TASK_KILLED is generated and the task launch is not forwarded to the executor. Note that if the task belongs
    * to a task group, killing of one task results in all tasks in the task group being killed. Mesos releases the
    * resources for a task once it receives a terminal update for the task. If the task is unknown to the master,
    * a TASK_LOST will be generated.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#kill
    */
  def newKill(taskId: TaskID, agentId: Option[AgentID] = None, killPolicy: Option[KillPolicy]): Call = {
    val killBuilder = Call.Kill
      .newBuilder()
      .setTaskId(taskId)
    agentId.foreach(killBuilder.setAgentId)
    killPolicy.foreach(killBuilder.setKillPolicy)

    Call
      .newBuilder()
      .setType(Call.Type.KILL)
      .setFrameworkId(frameworkId)
      .setKill(killBuilder)
      .build()
  }

  /**
    * Factory method to construct a SHUTDOWN Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to shutdown a specific custom executor. When an executor gets a shutdown event, it is
    * expected to kill all its tasks (and send TASK_KILLED updates) and terminate.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#shutdown
    */
  def newShutdown(executorId: ExecutorID, agentId: AgentID): Call = {
    Call
      .newBuilder()
      .setType(Call.Type.SHUTDOWN)
      .setFrameworkId(frameworkId)
      .setShutdown(
        Call.Shutdown
          .newBuilder()
          .setExecutorId(executorId)
          .setAgentId(agentId)
          .build())
      .build()
  }

  /**
    * Factory method to construct a ACKNOWLEDGE Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to acknowledge a status update. Note that with the new API, schedulers are responsible
    * for explicitly acknowledging the receipt of status updates that have status.uuid set. These status updates
    * are retried until they are acknowledged by the scheduler. The scheduler must not acknowledge status updates
    * that do not have `status.uuid` set, as they are not retried. The `uuid` field contains raw bytes encoded in Base64.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#acknowledge
    */
  def newAcknowledge(agentId: AgentID, taskId: TaskID, uuid: protobuf.ByteString): Call = {
    Call
      .newBuilder()
      .setType(Call.Type.ACKNOWLEDGE)
      .setFrameworkId(frameworkId)
      .setAcknowledge(
        Call.Acknowledge
          .newBuilder()
          .setAgentId(agentId)
          .setTaskId(taskId)
          .setUuid(uuid)
          .build())
      .build()
  }

  /**
    * Factory method to construct a RECONCILE Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to query the status of non-terminal tasks. This causes the master to send back UPDATE
    * events for each task in the list. Tasks that are no longer known to Mesos will result in TASK_LOST updates.
    * If the list of tasks is empty, master will send UPDATE events for all currently known tasks of the framework.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#reconcile
    */
  def newReconcile(tasks: Seq[Reconcile.Task]): Call = {
    val reconcileBuilder = Call.Reconcile.newBuilder()
    tasks.foreach(reconcileBuilder.addTasks)

    Call
      .newBuilder()
      .setType(Call.Type.RECONCILE)
      .setFrameworkId(frameworkId)
      .setReconcile(reconcileBuilder)
      .build()
  }

  /**
    * Factory method to construct a MESSAGE Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to send arbitrary binary data to the executor. Mesos neither interprets this data nor
    * makes any guarantees about the delivery of this message to the executor. data is raw bytes encoded in Base64
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#message
    */
  def newMessage(agentId: AgentID, executorId: ExecutorID, message: ByteString): Call = {
    Call
      .newBuilder()
      .setType(Call.Type.MESSAGE)
      .setFrameworkId(frameworkId)
      .setMessage(
        Call.Message
          .newBuilder()
          .setAgentId(agentId)
          .setExecutorId(executorId)
          .setData(protobuf.ByteString.copyFrom(message.toArray))
          .build())
      .build()
  }

  /**
    * Factory method to construct a REQUEST Mesos Call event. Calling this method has no side effects.
    *
    * Sent by the scheduler to request resources from the master/allocator. The built-in hierarchical allocator
    * simply ignores this request but other allocators (modules) can interpret this in a customizable fashion.
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#request
    */
  def newRequest(requests: Seq[Request]): Call = {
    val requestBuilder = Call.Request.newBuilder()
    requests.foreach(requestBuilder.addRequests)

    Call
      .newBuilder()
      .setType(Call.Type.REQUEST)
      .setFrameworkId(frameworkId)
      .setRequest(requestBuilder)
      .build()
  }

  /**
    * Factory method to construct a ACCEPT_INVERSE_OFFERS Mesos Call event. Calling this method has no side effects.
    *
    * Accepts an inverse offer. Inverse offers should only be accepted if the resources in the offer can be safely
    * evacuated before the provided unavailability.
    *
    * https://mesosphere.com/blog/mesos-inverse-offers/
    */
  def newAcceptInverseOffers(offers: Seq[OfferID], filters: Option[Filters] = None): Call = {
    val acceptInverseBuilder = Call.AcceptInverseOffers.newBuilder()
    offers.foreach(acceptInverseBuilder.addInverseOfferIds)
    filters.foreach(acceptInverseBuilder.setFilters)

    Call
      .newBuilder()
      .setType(Call.Type.ACCEPT_INVERSE_OFFERS)
      .setFrameworkId(frameworkId)
      .setAcceptInverseOffers(acceptInverseBuilder)
      .build()
  }

  /**
    * Factory method to construct a DECLINE_INVERSE_OFFERS Mesos Call event. Calling this method has no side effects.
    *
    * Declines an inverse offer. Inverse offers should be declined if
    * the resources in the offer might not be safely evacuated before
    * the provided unavailability.
    *
    * https://mesosphere.com/blog/mesos-inverse-offers/
    */
  def newDeclineInverseOffers(offers: Seq[OfferID], filters: Option[Filters] = None): Call = {
    val declineInverseBuilder = Call.DeclineInverseOffers.newBuilder()
    offers.foreach(declineInverseBuilder.addInverseOfferIds)
    filters.foreach(declineInverseBuilder.setFilters)

    Call
      .newBuilder()
      .setType(Call.Type.DECLINE_INVERSE_OFFERS)
      .setFrameworkId(frameworkId)
      .setDeclineInverseOffers(declineInverseBuilder)
      .build()
  }
}
