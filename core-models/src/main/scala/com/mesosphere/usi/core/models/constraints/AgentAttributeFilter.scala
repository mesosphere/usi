package com.mesosphere.usi.core.models.constraints

import java.util

import org.apache.mesos.v1.Protos

import collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

/**
  * An agent attribute filter that will only accept offers from agents that contain all required string attributes.
  *
  * The agent may contain more attributes.
  *
  * @param requiredAttributes The subset of agent attributes the agent *must* have.
  */
case class AgentStringAttributeFilter(requiredAttributes: Map[String, String]) extends AgentFilter with StrictLogging {

  /*
   * Java constructor.
   */
  def this(javaAttributes: util.HashMap[String, String]) = {
    this(requiredAttributes = javaAttributes.asScala.toMap)
  }

  override def apply(offer: Protos.Offer): Boolean = {
    val agentAttributes: Map[String, String] = offer.getAttributesList.asScala
      .filter(_.getType == Protos.Value.Type.TEXT)
      .map { attribute =>
        attribute.getName -> attribute.getText.getValue
      }(collection.breakOut)

    requiredAttributes.forall {
      case (key, value) =>
        agentAttributes.get(key).contains(value)
    }
  }
}
