package com.mesosphere.utils.mesos

object MesosFormats {
  import MesosFacade._
  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  implicit class FormatWithDefault[A](val m: OFormat[Option[A]]) extends AnyVal {
    def withDefault(a: A): OFormat[A] = m.inmap(_.getOrElse(a), Some(_))
  }

  implicit lazy val ITResourceScalarValueFormat: Format[ITResourceScalarValue] = Format(
    Reads.of[Double].map(ITResourceScalarValue(_)),
    Writes(scalarValue => JsNumber(scalarValue.value))
  )

  implicit lazy val ITResourcePortValueFormat: Format[ITResourceStringValue] = Format(
    Reads.of[String].map(ITResourceStringValue(_)),
    Writes(portValue => JsString(portValue.portString))
  )

  implicit lazy val ITResourceValueFormat: Format[ITResourceValue] = Format(
    Reads[ITResourceValue] {
      case JsNumber(value) => JsSuccess(ITResourceScalarValue(value.toDouble))
      case JsString(value) => JsSuccess(ITResourceStringValue(value))
      case _ => JsError("expected string or number")
    },
    Writes[ITResourceValue] {
      case ITResourceScalarValue(value) => JsNumber(value)
      case ITResourceStringValue(value) => JsString(value)
    }
  )

  implicit lazy val ITAttributesFormat: Format[ITAttributes] = Format(
    Reads.of[Map[String, ITResourceValue]].map(ITAttributes(_)),
    Writes[ITAttributes](a => Json.toJson(a.attributes))
  )

  implicit lazy val ITResourcesFormat: Format[ITResources] = Format(
    Reads.of[Map[String, ITResourceValue]].map(ITResources(_)),
    Writes[ITResources](resources => Json.toJson(resources.resources))
  )

  implicit lazy val ITAgentFormat: Format[ITAgent] = (
    (__ \ "id").format[String] ~
      (__ \ "attributes").formatNullable[ITAttributes].withDefault(ITAttributes.empty) ~
      (__ \ "resources").formatNullable[ITResources].withDefault(ITResources.empty) ~
      (__ \ "used_resources").formatNullable[ITResources].withDefault(ITResources.empty) ~
      (__ \ "offered_resources").formatNullable[ITResources].withDefault(ITResources.empty) ~
      (__ \ "reserved_resources").formatNullable[Map[String, ITResources]].withDefault(Map.empty) ~
      (__ \ "unreserved_resources").formatNullable[ITResources].withDefault(ITResources.empty)
  )(ITAgent.apply, unlift(ITAgent.unapply))

  implicit lazy val ITStatusFormat: Format[ITMesosState] = (
    (__ \ "version").format[String] ~
      (__ \ "git_sha").formatNullable[String] ~
      (__ \ "slaves").format[Seq[ITAgent]] ~
      (__ \ "frameworks").format[Seq[ITFramework]] ~
      (__ \ "completed_frameworks").format[Seq[ITFramework]] ~
      (__ \ "unregistered_frameworks").format[Seq[String]]
  )(ITMesosState.apply, unlift(ITMesosState.unapply))

  implicit lazy val ITaskFormat: Format[ITask] = Json.format[ITask]

  implicit lazy val ITFrameworkFormat: Format[ITFramework] = Json.format[ITFramework]

  implicit lazy val ITFrameworksFormat: Format[ITFrameworks] = Json.format[ITFrameworks]
}
