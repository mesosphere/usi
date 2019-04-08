package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.{BidiShape, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.scaladsl.{BidiFlow, Broadcast, Flow, GraphDSL, Sink, Source}
import com.mesosphere.mesos.client.{MesosCalls, MesosClient}
import com.mesosphere.usi.core.models.{
  PodRecordUpdated,
  SpecEvent,
  SpecUpdated,
  SpecsSnapshot,
  StateEvent,
  StateSnapshot,
  StateUpdated
}
import com.mesosphere.usi.repository.PodRecordRepository
import java.util.concurrent.atomic.AtomicInteger
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import scala.collection.JavaConverters._

/*
 * Provides the scheduler graph component. The component has two inputs, and two outputs:
 *
 * Input:
 * 1) SpecInput - Used to replicate the specification state from the framework implementation to the USI SchedulerLogic;
 *    First, a spec snapshot, followed by spec updates.
 * 2) MesosEvents - Events from Mesos; offers, task status updates, etc.
 *
 * Output:
 * 1) StateEvents - Used to replicate the state of pods, agents, and reservations to the framework
 *    First, a scheduler state snapshot, followed by state updates.
 * 2) MesosCalls - Actions, such as revive, kill, accept offer, etc., used to realize the specification.
 *
 * Fully wired, the graph looks like this at a high-level view:
 *
 *                                                 *** SCHEDULER ***
 *                    +------------------------------------------------------------------------+
 *                    |                                                                        |
 *                    |  +------------------+           +-------------+        StateOutput     |
 *        SpecInput   |  |                  |           |             |     /------------------>----> (framework)
 * (framework) >------>-->                  | Scheduler |             |    /                   |
 *                    |  |                  |  Events   |             |   /                    |
 *                    |  |  SchedulerLogic  o-----------> Persistence o--+                     |
 *                    |  |                  |           |             |   \                    |
 *       MesosEvents  |  |                  |           |             |    \   MesosCalls      |
 *       /------------>-->                  |           |             |     \------------------>
 *      /             |  |                  |           |             |                        |\
 *     /              |  +------------------+           +-------------+                        | \
 *    /               |                                                                        |  \
 *   |                +------------------------------------------------------------------------+  |
 *    \                                                                                           |
 *     \                               +----------------------+                                  /
 *      \                              |                      |                                 /
 *       \-----------------------------<        Mesos         <---------------------------------
 *                                     |                      |
 *                                     +----------------------+
 */
object Scheduler {
  type SpecInput = (SpecsSnapshot, Source[SpecUpdated, Any])

  type StateOutput = (StateSnapshot, Source[StateEvent, Any])

  def fromClient(
      client: MesosClient,
      podRecordRepository: PodRecordRepository): Flow[SpecInput, StateOutput, NotUsed] = {
    if (!isMultiRoleFramework(client.frameworkInfo)) {
      throw new IllegalArgumentException(
        "USI scheduler provides support for MULTI_ROLE frameworks only. " +
          "Please provide a MesosClient with FrameworkInfo that has capability MULTI_ROLE")
    }
    fromFlow(client.calls, podRecordRepository, Flow.fromSinkAndSource(client.mesosSink, client.mesosSource))
  }

  def fromFlow(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository,
      mesosFlow: Flow[MesosCall, MesosEvent, Any]): Flow[SpecInput, StateOutput, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(unconnectedGraph(mesosCallFactory, podRecordRepository), mesosFlow)((_, _) => NotUsed) {
        implicit builder =>
          { (graph, mesos) =>
            import GraphDSL.Implicits._

            mesos ~> graph.in2
            graph.out2 ~> mesos

            FlowShape(graph.in1, graph.out1)
          }
      }
    }
  }

  /**
    * We express an interface of receiving a snapshot, followed by a series of events, to guide consumers of USI down a
    * proper implementation path. A SpecSnapshot should be the very first thing that the USI Scheduler receives from the
    * Framework implementation.
    *
    * However, for convenience in working with streams, internally we deal with a single stream of SpecEvents.
    */
  private val specInputFlatteningFlow: Flow[SpecInput, SpecEvent, NotUsed] = Flow[SpecInput].flatMapConcat {
    case (snapshot, rest) =>
      rest.prepend(Source.single(snapshot))
  }

  private val stateOutputBreakoutFlow: Flow[StateEvent, StateOutput, NotUsed] = Flow[StateEvent].prefixAndTail(1).map {
    case (Seq(snapshot), stateEvents) =>
      val stateSnapshot = snapshot match {
        case x: StateSnapshot => x
        case _ => throw new IllegalStateException("First event is allowed to be only a state snapshot")
      }
      val stateUpdates = stateEvents.map {
        case c: StateUpdated => c
        case _: StateSnapshot =>
          throw new IllegalStateException("Only the first event is allowed to be a state snapshot")
      }
      (stateSnapshot, stateUpdates)
  }

  private[core] def unconnectedGraph(
      mesosCallFactory: MesosCalls,
      podRecordRepository: PodRecordRepository): BidiFlow[SpecInput, StateOutput, MesosEvent, MesosCall, NotUsed] = {
    BidiFlow.fromGraph {
      GraphDSL.create(
        new SchedulerLogicGraph(mesosCallFactory, podRecordRepository.readAll()),
      ) { implicit builder => (schedulerLogic) =>
        {
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[SchedulerEvents](2, eagerCancel = true))
          val specInputFlattening = builder.add(specInputFlatteningFlow)
          val stateOutputBreakout = builder.add(stateOutputBreakoutFlow)

          val persistenceStorageFlow = builder.add(persistenceFlow(podRecordRepository))
          specInputFlattening ~> schedulerLogic.in0
          schedulerLogic.out ~> persistenceStorageFlow ~> broadcast.in

          val mesosCalls = broadcast.out(0).mapConcat { frameResult =>
            frameResult.mesosCalls
          }
          val stateEvents = broadcast.out(1).mapConcat { frameResult =>
            frameResult.stateEvents
          }

          stateEvents ~> stateOutputBreakout

          BidiShape.apply(specInputFlattening.in, stateOutputBreakout.outlet, schedulerLogic.in1, mesosCalls.outlet)
        }
      }
    }
  }

  private def persistenceFlow(
      podRecordRepository: PodRecordRepository
  ): Flow[SchedulerEvents, SchedulerEvents, NotUsed] = {
    Flow.fromGraph(new GraphStage[FlowShape[SchedulerEvents, SchedulerEvents]] {

      private val inlet = Inlet[SchedulerEvents]("scheduler-events-inlet")
      private val outlet = Outlet[SchedulerEvents]("scheduler-events-outlet")

      override def shape: FlowShape[SchedulerEvents, SchedulerEvents] =
        new FlowShape[SchedulerEvents, SchedulerEvents](inlet, outlet)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
        new GraphStageLogic(shape) {
          private[this] var nextElement: Option[SchedulerEvents] = None
          val eventCounter = new AtomicInteger(0)

          val maybePushToPull = this.getAsyncCallback[Unit] { _ =>
            if (nextElement.isDefined && isAvailable(outlet)) {
              push(outlet, nextElement.get)
              nextElement = None
            }
            if (nextElement.isEmpty && !hasBeenPulled(inlet)) {
              pull(inlet)
            }
          }

          setHandler(
            inlet,
            new InHandler {
              override def onPush(): Unit = {
                if (nextElement.isEmpty) {
                  val schedulerEvents = grab(inlet)
                  val (storeRecords, deleteRecords) = schedulerEvents.stateEvents.collect {
                    case x: PodRecordUpdated => x
                  }.partition(_.newRecord.isDefined)
                  eventCounter.addAndGet(storeRecords.size + deleteRecords.size)
                  nextElement = Some(schedulerEvents)
                  if (eventCounter.get() == 0) {
                    maybePushToPull.invoke(())
                  } else {
                    Source(storeRecords)
                      .map(_.newRecord.get)
                      .via(podRecordRepository.storeFlow)
                      .runWith(Sink.foreach(_ => {
                        if (eventCounter.decrementAndGet() == 0) maybePushToPull.invoke(())
                      }))(materializer)
                    Source(deleteRecords)
                      .map(_.id)
                      .via(podRecordRepository.deleteFlow)
                      .runWith(Sink.foreach(_ => {
                        if (eventCounter.decrementAndGet() == 0) maybePushToPull.invoke(())
                      }))(materializer)
                  }
                }
              }
            }
          )

          setHandler(
            outlet,
            new OutHandler {
              override def onPull(): Unit = if (eventCounter.get() == 0) maybePushToPull.invoke(())
            }
          )

          override def preStart(): Unit = {
            pull(inlet)
          }
        }
      }
    })
  }

  private def isMultiRoleFramework(frameworkInfo: FrameworkInfo): Boolean =
    frameworkInfo.getCapabilitiesList.asScala.exists(_.getType == FrameworkInfo.Capability.Type.MULTI_ROLE)
}
