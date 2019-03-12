package com.mesosphere.usi.core

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.logic.{MesosEventsLogic, SpecLogic}
import com.mesosphere.usi.core.models.{PodId, SpecEvent}
import org.apache.mesos.v1.scheduler.Protos.{Call, Event => MesosEvent}

/**
  * Container class responsible for keeping track of the state and cache.
  *
  * ## SpecificationState vs SchedulerLogic state
  *
  * The SchedulerLogic has two pieces of state: SpecificationState and SchedulerState (records and statuses). The
  * SchedulerLogic maintains the latter. All manipulation to the SchedulerLogic state is done via one of the two
  * processes:
  *
  * - The specification state is updated through incoming SpecEvents (we consume and replicate the
  * framework-implementation's specifications)
  * - The SchedulerState (statuses, records, etc.) is updated through StateEvents that returned as intents
  *
  * As such, it's worth emphasizing that business logic does not have any direct side-effects, and it manipulates the
  * SchedulerState only by returning intents. This allows us the following:
  *
  * - It's more efficient, since it saves us the trouble of diffing a large data-structure with each update
  * - We have built-in guarantees that evolutions to the SchedulerState can be reliably replicated by processing these
  * events, since they led to the changes in the first place
  * - We can easily know which portions of the SchedulerState should be persisted during the persistence layer.
  * - It restricts, via the type system, the portions of the state the business logic is allowed (IE: it
  * would be illegal for the business logic to update a specification)
  *
  * ## Intents and Events
  *
  * In the SchedulerLogic code, we'll use the word intents and events. Intents are things not yet applied, and should
  * be. Events are things that were applied and we're notifying you about.
  *
  * In the SchedulerLogic, a StateEvent is used both to manipulate the SchedulerState (similar to how event-sourced
  * persistent actors evolve their state), and is also used to describe the evolution (so that the state can be
  * incrementally persisted and followed). In the SchedulerLogic, we'll refer to a StateEvent as an intent until it is
  * applied, after-which it will be called an event. Mesos calls will be referred to as intents as they are not applied
  * until they are published to the Mesos Master.
  *
  * ## Concept of a "Frame"
  *
  * Visualized:
  *
  * [Incoming Event: offer match]
  *      |
  *      v
  * (beginning of frame)
  *   apply pod launch logic:
  *     create an intent for Mesos accept offer call for matched pending launch pods
  *     specify the existence of a podRecord, with the launch time and agentId
  *   update internal cache
  *   process revive / suppress need
  * (end of frame)
  *      |
  *      v
  * [Emit all state events and Mesos call intents]
  *
  * In the SchedulerLogic, each event is processed (either a Specification updated event, or a Mesos event) in what we
  * call a "frame". During the processing of a single event (such as an offer), the SchedulerLogic will be updated
  * through the application of several functions. The result of that event will lead to the emission of several
  * stateEvents and Mesos intents, which will be accumulated and emitted via a data structure we call the FrameResult.
  */
private[core] class SchedulerLogicHandler(mesosCallFactory: MesosCalls) {

  private val schedulerLogic = new SpecLogic(mesosCallFactory)
  private val mesosEventsLogic = new MesosEventsLogic(mesosCallFactory)

  /**
    * Our view of the framework-implementations specifications, which we replicate by consuming Specification events
    */
  private var specs: SpecState = SpecState.empty

  /**
    * State managed by the SchedulerLogicHandler (records and statuses). Statuses are derived from Mesos events. Records
    * contain persistent, non-recoverable facts from Mesos, such as pod-launched time, agentId on which a pod was
    * launched, agent information or the time at which a pod was first seen as unhealthy or unreachable.
    */
  private var state: SchedulerState = SchedulerState.empty

  /**
    * Cached view of SchedulerState and SpecificationState. We incrementally update this computation at the end of each
    * frame based on podIds becoming dirty.
    */
  private var cachedPendingLaunch = CachedPendingLaunch(Set.empty)

  def handleSpecEvent(msg: SpecEvent): SchedulerEvents = {
    handleFrame { builder =>
      builder
        .applySpecEvent(msg)
        .process { (specs, state, dirtyPodIds) =>
          schedulerLogic.computeNextStateForPods(specs, state)(dirtyPodIds)
        }
    }
  }

  /**
    * Instantiate a frameResultBuilder instance, call the handler, then follow up with housekeeping:
    *
    * - Prune terminal / unreachable podStatuses for which no podSpec is defined
    * - Update the pending launch set index / cache
    * - (WIP) issue any revive calls (this should be done elsewhere)
    *
    * @return The total state effects applied over the life-cycle of this state evaluation.
    */
  private def handleFrame(fn: FrameResultBuilder => FrameResultBuilder): SchedulerEvents = {
    val frameResultBuilder = fn(FrameResultBuilder.givenState(this.specs, this.state)).process {
      (specs, state, dirtyPodIds) =>
        pruneTaskStatuses(specs, state)(dirtyPodIds)
    }.process(updateCachesAndRevive)

    // update our state for the next frame processing
    this.state = frameResultBuilder.state
    this.specs = frameResultBuilder.specs

    // Return our result
    frameResultBuilder.result
  }

  def generateSuppressCalls(pendingLaunch: Set[PodId], newLaunched: Set[PodId]): List[Call] = {
    val alreadyLaunchedRoles = newLaunched
      .map(id => specs.podSpecs.get(id))
      .collect { case Some(p) => p.runSpec.roles }
      .flatten

    val rolesBeingLaunched = pendingLaunch
      .map(id => specs.podSpecs.get(id))
      .collect { case Some(p) => p.runSpec.roles }
      .flatten

    alreadyLaunchedRoles.diff(rolesBeingLaunched).map(r => mesosCallFactory.newSuppress(Some(r))).toList
  }

  private def updateCachesAndRevive(
      specs: SpecState,
      state: SchedulerState,
      dirtyPodIds: Set[PodId]): SchedulerEvents = {
    val updateResult = this.cachedPendingLaunch.update(specs, state, dirtyPodIds)
    this.cachedPendingLaunch = updateResult.newCachedPendingLaunch
    val reviveCalls = updateResult.newToBeLaunched
      .map(id => specs.podSpecs.get(id))
      .collect { case Some(p) => p.runSpec.roles }
      .flatten
      .map(r => mesosCallFactory.newRevive(Some(r)))
      .toList

    val suppressCalls = generateSuppressCalls(cachedPendingLaunch.pendingLaunch, updateResult.newLaunched)

    SchedulerEvents(mesosCalls = reviveCalls ++ suppressCalls)
  }

  /**
    * Process a Mesos event and update internal state.
    *
    * @param event
    * @return The events describing state changes as Mesos call intents
    */
  def handleMesosEvent(event: MesosEvent): SchedulerEvents = {
    handleFrame { builder =>
      builder.process { (specs, state, _) =>
        mesosEventsLogic.processEvent(specs, state, cachedPendingLaunch.pendingLaunch)(event)
      }
    }
  }

  /**
    * We remove a task if it is not reachable and running, and it has no podSpec defined
    *
    * Should be called with the effects already applied for the specified podIds
    *
    * @param podIds podIds changed during the last state
    * @return
    */
  private def pruneTaskStatuses(specs: SpecState, state: SchedulerState)(podIds: Set[PodId]): SchedulerEvents = {
    podIds.iterator.filter { podId =>
      state.podStatuses.contains(podId)
    }.filter { podId =>
      val podSpecDefined = !specs.podSpecs.contains(podId)
      // prune terminal statuses for which there's no defined podSpec
      !podSpecDefined && state.podStatuses(podId).isTerminalOrUnreachable
    }.foldLeft(SchedulerEventsBuilder.empty) { (effects, podId) =>
        effects.withPodStatus(podId, None)
      }
      .result
  }
}
