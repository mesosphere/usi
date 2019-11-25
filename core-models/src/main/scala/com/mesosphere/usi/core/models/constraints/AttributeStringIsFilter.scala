package com.mesosphere.usi.core.models.constraints

import org.apache.mesos.v1.Protos

import collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

/**
  * An agent attribute filter which exactly compares two strings (including case)
  *
  * @param attributeName the name of the attribute to compare
  * @param value The value of the attribute.
  */
case class AttributeStringIsFilter(attributeName: String, value: String) extends AgentFilter with StrictLogging {
  override def apply(offer: Protos.Offer): Boolean = {
    offer.getAttributesList.iterator.asScala.exists { attribute =>
      attribute.getName() == attributeName &&
      attribute.getType == Protos.Value.Type.TEXT &&
      attribute.getText.getValue() == value
    }
  }

  override def description: String = s"attribute ${attributeName}'s string value is exactly '${value}'"
}
