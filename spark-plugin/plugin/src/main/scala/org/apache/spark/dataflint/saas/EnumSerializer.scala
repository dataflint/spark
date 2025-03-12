package org.apache.spark.dataflint.saas

import org.apache.spark.dataflint.EnumValue
import org.json4s.{JInt, JString}
import org.json4s.reflect.TypeInfo
import org.json4s.{Formats, JValue, MappingException, Serializer}

import scala.reflect.ClassTag

// copied from json4s source code, because some spark version depends on json4s versions without this class
class EnumSerializer[E <: Enumeration: ClassTag](enumeration: E) extends Serializer[EnumValue[E]] {
  import org.json4s.JsonDSL._

  private[this] val EnumerationClass = classOf[Enumeration#Value]

  private[this] def isValid(json: JValue) = json match {
    case JInt(value) => enumeration.values.toSeq.map(_.id).contains(value.toInt)
    case _ => false
  }

  private[this] def enumerationValueToEnumValueOfE(value: enumeration.Value): EnumValue[E] =
    value.asInstanceOf[EnumValue[E]]

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), EnumValue[E]] = {
    case (TypeInfo(EnumerationClass, _), json) if isValid(json) =>
      json match {
        case JInt(value) => enumerationValueToEnumValueOfE(enumeration(value.toInt))
        case value => throw new MappingException(s"Can't convert $value to $EnumerationClass")
      }
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case i: Enumeration#Value if enumeration.values.exists(_ == i) => i.id
  }
}

class EnumNameSerializer[E <: Enumeration: ClassTag](enumeration: E) extends Serializer[EnumValue[E]] {
  import org.json4s.JsonDSL._

  private[this] val EnumerationClass = classOf[Enumeration#Value]

  private[this] def enumerationValueToEnumValueOfE(value: enumeration.Value): EnumValue[E] =
    value.asInstanceOf[EnumValue[E]]

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), EnumValue[E]] = {
    case (_ @TypeInfo(EnumerationClass, _), json) if isValid(json) => {
      json match {
        case JString(value) => enumerationValueToEnumValueOfE(enumeration.withName(value))
        case value => throw new MappingException(s"Can't convert $value to $EnumerationClass")
      }
    }
  }

  private[this] def isValid(json: JValue) = json match {
    case JString(value) if enumeration.values.exists(_.toString == value) => true
    case _ => false
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case i: Enumeration#Value if enumeration.values.exists(_ == i) => i.toString
  }
}