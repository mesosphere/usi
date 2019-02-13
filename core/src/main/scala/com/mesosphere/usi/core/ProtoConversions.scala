package com.mesosphere.usi.core

import com.mesosphere.usi.core.models.{AgentId, TaskId}
import org.apache.mesos.v1.{Protos => Mesos}
import scala.collection.immutable.NumericRange

private[usi] object ProtoConversions {
  implicit class AgentIdProtoConversions(agentId: AgentId) {
    def asProto: Mesos.AgentID =
      ProtoBuilders.newAgentId(agentId.value)
  }
  implicit class AgentIDModelConversions(agentId: Mesos.AgentID) {
    def asModel: AgentId = AgentId(agentId.getValue)
  }

  implicit class TaskIdProtoConversions(taskId: TaskId) {
    def asProto: Mesos.TaskID =
      ProtoBuilders.newTaskId(taskId.value)
  }

  implicit class SetStringAsValueSet(set: Iterable[String]) {
    def asProtoSet: Mesos.Value.Set = {
      val b = Mesos.Value.Set.newBuilder()
      set.foreach(b.addItem)
      b.build()
    }
  }

  implicit class NumericAsValueScalar[T: Numeric](value: T) {
    def asProtoScalar: Mesos.Value.Scalar = {
      Mesos.Value.Scalar.newBuilder().setValue(implicitly[Numeric[T]].toDouble(value)).build
    }
  }

  implicit class RangeAsValueRange[T: Numeric](value: NumericRange.Inclusive[T])(implicit n: Numeric[T]) {
    def asProtoRange: Mesos.Value.Range = {
      Mesos.Value.Range.newBuilder
        .setBegin(n.toLong(value.head))
        .setEnd(n.toLong(value.last))
        .build()
    }
  }
}
