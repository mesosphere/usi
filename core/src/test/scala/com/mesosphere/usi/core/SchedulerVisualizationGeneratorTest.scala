package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.helpers.MockedFactory
import com.mesosphere.usi.core.models.{PodSpecUpdatedEvent, StateSnapshot}
import com.mesosphere.utils.UnitTest
import org.scalatest.Inside
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.reflect.runtime.universe._
import net.mikolak.travesty
import net.mikolak.travesty.OutputFormat
import net.mikolak.travesty.registry._

class SchedulerVisualizationGeneratorTest extends UnitTest with Inside {

  /**
    * Returns a flow with annotated type inputs and outputs for graph visualization
    */
  implicit def fakeFlow[A <: Any: TypeTag, B <: AnyRef: TypeTag]: Flow[A, B, NotUsed] = {
    Flow[A].register.map { _ =>
      ???
    }.asInstanceOf[Flow[A, B, NotUsed]]
      .register
  }

  "generate a graph of the scheduler" in {
    // this actually isn't a test; it generates the file docs/scheduler-stream.svg based on the actual graph structure in code
    val factory = new SchedulerLogicFactory with PersistenceFlowFactory with SuppressReviveFactory {
      override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] =
        fakeFlow[SchedulerEvents, SchedulerEvents].named("persistenceFlow")

      override def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, MesosCall, NotUsed] = {
        fakeFlow[PodSpecUpdatedEvent, MesosCall].named("suppressAndReviveFlow")
      }

      override def newSchedulerLogicGraph(snapshot: StateSnapshot): SchedulerLogicGraph =
        MockedFactory().newSchedulerLogicGraph(StateSnapshot.empty)
    }
    val mesosMaster = fakeFlow[MesosCall, MesosEvent].named("mesos-master")

    val graph = Scheduler
      .schedulerGraph(StateSnapshot.empty, factory)
      .join(mesosMaster)

    travesty.toFile(graph, OutputFormat.SVG)("../docs/scheduler-stream.svg")
  }
}
