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
    this(requiredAttributes = javaAttributes)
  }

  override def apply(offer: Protos.Offer): Boolean = {
    val agentAttributes: Map[String, String] = offer.getAttributesList.asScala.filter(_.getType == Protos.Value.Type.TEXT)
      .map { attribute => attribute.getName -> attribute.getText.getValue }(collection.breakOut)

    requiredAttributes.forall { case (key, value) =>
      agentAttributes.get(key).contains(value)
    }
  }
}

case class AgentAttributeFilter(attributes: Map[String, Any]) extends AgentFilter with StrictLogging {

  // Entrypoint for Java as JavaConverters.mapAsScalaMap only supports as mutable Map.
  def this(javaAttributes: scala.collection.mutable.Map[String, Any]) = {
    this(attributes = javaAttributes.toMap)
  }

  override def apply(offer: Protos.Offer): Boolean = {

    // Everything with offer herein relates to its attributes.
    val offerList = offer.getAttributesList.asScala
    val offerMap = offerList.map(attribute => (attribute.getName -> getAttributeValue(attribute))).toMap

    // Keys common to both offer and attributes
    val commonKeys = offerMap.keySet intersect attributes.keySet

    logger.info(s"DELTEME@kjoshi AgentAttributeFilter\ncommonKeys: [${commonKeys
      .mkString(",")}]\n attributes:[${attributes.mkString(",")}]\noffer:[${offerMap.mkString(",")}]")

    // Ensure all the values are equal.
    val valuesMatch = commonKeys.map(key => attributes(key).equals(offerMap(key))).foldLeft(true)((x, y) => x && y)

    // The filter is true iff all the keys and their values match.
    val attributeMatchResult = commonKeys.size == attributes.size && valuesMatch

    logger.info(s"DELTEME@kjoshi AgentAttributeFilter result: $attributeMatchResult on Offer: ${offer.getId.getValue}")

    attributeMatchResult
  }

  private def getAttributeValue(attribute: Protos.Attribute) = {
    attribute.getType() match {
      //case Protos.Value.Type.TEXT => attribute.getText().toString;
      case Protos.Value.Type.TEXT => attribute.getTextOrBuilder().getValue()
      case _ =>
        ???

      // TODO: Add support for the following
      //case Protos.Value.Type.SCALAR
      //case Protos.Value.Type.SET
      //case Protos.Value.Type.RANGES
    }
  }
}
