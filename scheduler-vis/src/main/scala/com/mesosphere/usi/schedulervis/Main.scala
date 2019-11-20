package com.mesosphere.usi.schedulervis

import akka.NotUsed
import akka.stream.{FanInShape2, Graph}
import akka.stream.scaladsl.{Flow, ZipLatestWith}
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.{
  PersistenceFlowFactory,
  Scheduler,
  SchedulerEvents,
  SchedulerLogicFactory,
  SuppressReviveFactory
}
import com.mesosphere.usi.core.models.{PodSpecUpdatedEvent, StateSnapshot}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.reflect.runtime.universe._
import net.mikolak.travesty
import net.mikolak.travesty.OutputFormat
import net.mikolak.travesty.registry._

object Main extends App {
  println("hello")

  /**
    * Returns a flow with annotated type inputs and outputs for graph visualization
    */
  implicit def fakeFlow[A <: Any: TypeTag, B <: AnyRef: TypeTag]: Flow[A, B, NotUsed] = {
    Flow[A].register.map { _ =>
      ???
    }.asInstanceOf[Flow[A, B, NotUsed]].register
  }

  // this actually isn't a test; it generates the file docs/scheduler-stream.svg based on the actual graph structure in code
  val factory = new SchedulerLogicFactory with PersistenceFlowFactory with SuppressReviveFactory {
    override def newPersistenceFlow(): Flow[SchedulerEvents, SchedulerEvents, NotUsed] =
      fakeFlow[SchedulerEvents, SchedulerEvents].named("persistenceFlow")

    override def newSuppressReviveFlow: Flow[PodSpecUpdatedEvent, MesosCall, NotUsed] = {
      fakeFlow[PodSpecUpdatedEvent, MesosCall].named("suppressAndReviveFlow")
    }

    override private[usi] def newSchedulerLogicGraph(
        snapshot: StateSnapshot): Graph[FanInShape2[SchedulerCommand, MesosEvent, SchedulerEvents], NotUsed] =
      ZipLatestWith
        .apply[SchedulerCommand, MesosEvent, SchedulerEvents](zipper = (_, _) => ???)
        .named("SchedulerLogicGraph")
  }

  val mesosMaster = fakeFlow[MesosCall, MesosEvent].named("mesos-master")

  val graph = Scheduler
    .schedulerGraph(StateSnapshot.empty, factory)
    .join(mesosMaster)

  println("Generating docs/scheduler-stream.svg")
  travesty.toFile(graph, OutputFormat.SVG)("../scheduler-stream.svg")
  println("Done")
}
