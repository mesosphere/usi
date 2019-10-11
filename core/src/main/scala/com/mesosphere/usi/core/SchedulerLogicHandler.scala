package com.mesosphere.usi.core

import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.logic.{MesosEventsLogic, SpecLogic}
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{
  PodId,
  PodSpec,
  PodSpecUpdatedEvent,
  PodStatusUpdatedEvent,
  RunningPodSpec,
  StateSnapshot
}
import org.apache.mesos.v1.scheduler.Protos.{Call, Event => MesosEvent}

/**
  * Container class responsible for keeping track of the scheduler state.
  *
  * The SchedulerLogicHandler maintains its own state for launching pods. The state is manipulated by the implementation
  * framework by providing SchedulerCommand objects, such as LaunchPod or KillPod.
  *
  * All logic is implemented as pure functions which make decisions about what to do given an incoming Mesos event or
  * SchedulerCommand. These functions return state events which both update the state and are used to advertise changes
  * to the implementation framework, and the functions also return Mesos calls intended to be published to the Mesos
  * Master.
  *
  * The motivation for this approach is:
  *
  * - Efficiency: Manipulating the state through events makes it efficient to know what has changed, so we can react to
  *   change effeciently without diffing a potentially-large data structure.
  * - Evolutions to the SchedulerState can be reliably replicated by processing these events, since they led to the
  * changes in the first place
  * - We can easily know which portions of the SchedulerState should be persisted during the persistence layer.
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
  * In the SchedulerLogic, each event is processed (either a scheduler command, or a Mesos event) in what we call a
  * "frame". During the processing of a single event (such as an offer), the SchedulerLogic will be updated through the
  * application of several functions. The result of that event will lead to the emission of several stateEvents and
  * Mesos intents, which will be accumulated and emitted via a data structure we call the FrameResult.
  *
  * All record events in a frame are persisted before any of the events are exposed to the outside world. This means
  * that a Mesos call will not be processed until after we store the associated PodRecord recording the offer chosen for
  * a given RunningPodSpec
  */
private[core] class SchedulerLogicHandler(mesosCallFactory: MesosCalls, initialState: StateSnapshot) {

  private val schedulerLogic = new SpecLogic(mesosCallFactory)
  private val mesosEventsLogic = new MesosEventsLogic(mesosCallFactory)

  /**
    * State managed by the SchedulerLogicHandler. Statuses are derived from Mesos events.
    */
  private var state: SchedulerState = SchedulerState.fromSnapshot(initialState)

  def handleCommand(command: SchedulerCommand): SchedulerEvents = {
    handleFrame { builder =>
      builder.process { (state, _) =>
        schedulerLogic.handleCommand(state)(command)
      }
    }
  }

  /**
    * Instantiate a frameResultBuilder instance, call the handler, then follow up with housekeeping:
    *
    * - Prune terminal / unreachable podStatuses for which no podRecord is defined
    * - Prune TerminalPodSpecs for which we've received terminal status updates
    * - Issue suppress / revive calls
    *
    * @return The total state effects applied over the life-cycle of this state evaluation.
    */
  private def handleFrame(handler: FrameResultBuilder => FrameResultBuilder): SchedulerEvents = {
    val frameResultBuilder = handler(FrameResultBuilder.givenState(this.state))
      .process(prunePodStatuses)
      .process(pruneKilledTerminalSpecs)
      .process(suppressAndRevive(state.podSpecs))

    // update our state for the next frame processing
    this.state = frameResultBuilder.state

    // Return our result
    frameResultBuilder.result
  }

  /**
    * Remove all TerminalPodSpec instances that either don't have a podStatus, or have a terminal podStatus.
    *
    * Should be called with the effects already applied for the specified podIds
    *
    * @param changedPodIds podIds changed during the last state
    * @return
    */
  private[core] def pruneKilledTerminalSpecs(state: SchedulerState, changedPodIds: Set[PodId]): SchedulerEvents = {
    changedPodIds.iterator.flatMap { podId =>
      state.podSpecs.get(podId)
    }.filter { _.shouldBeTerminal }.filter { podSpec =>
      state.podStatuses.get(podSpec.id).forall(_.isTerminalOrUnreachable)

    }.foldLeft(SchedulerEventsBuilder.empty) { (events, pod) =>
        events.withStateEvent(PodSpecUpdatedEvent(pod.id, None))
      }
      .result
  }

  /**
    * Clean up all terminal-like podStatuses which don't have podRecords
    */
  private[core] def prunePodStatuses(state: SchedulerState, changedPodIds: Set[PodId]): SchedulerEvents = {
    changedPodIds.iterator.filter { podId =>
      state.podStatuses.get(podId).exists(_.isTerminalOrUnreachable)
    }
    // prune pod statuses if there is no podRecord associated with it
    .filterNot { podId =>
      state.podRecords.contains(podId)
    }.foldLeft(SchedulerEventsBuilder.empty) { (events, podId) =>
        events.withStateEvent(PodStatusUpdatedEvent(podId, None))
      }
      .result
  }

  /**
    * Given the state at the beginning of the frame, emit the appropriate suppress and revived functions based on new or
    * removed podSpecs
    *
    * @param oldPodSpecs The podSpecs state at the very beginning of the frame
    */
  private def suppressAndRevive(
      oldPodSpecs: Map[PodId, PodSpec])(state: SchedulerState, dirtyPodIds: Set[PodId]): SchedulerEvents = {

    val oldPodSpecIds = oldPodSpecs.keySet
    val newPodSpecRoles = state.podSpecs.valuesIterator.filterNot { podSpec =>
      oldPodSpecIds.contains(podSpec.id)
    }.collect { case RunningPodSpec(_, runSpec) => runSpec.role }.toSet

    val oldRoles: Set[String] =
      oldPodSpecs.valuesIterator.collect { case RunningPodSpec(_, runSpec) => runSpec.role }.toSet
    val currentLaunchingRoles =
      state.podSpecs.valuesIterator.collect { case RunningPodSpec(_, runSpec) => runSpec.role }.toSet
    val rolesNoLongerWanted = oldRoles -- currentLaunchingRoles

    val reviveCalls: List[Call] = newPodSpecRoles
      .map(r => mesosCallFactory.newRevive(Some(r)))(collection.breakOut)

    val suppressCalls = rolesNoLongerWanted.map { r =>
      mesosCallFactory.newSuppress(Some(r))
    }.toList

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
      builder.process { (state, _) =>
        mesosEventsLogic.processEvent(state)(event)
      }
    }
  }
}
