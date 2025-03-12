package org.apache.spark.dataflint.saas

import org.json4s.CustomSerializer
import org.json4s.JString

// copied from json4s source code, because some spark version depends on json4s versions without this class
class JavaEnumNameSerializer[E <: Enum[E]](implicit
                                           ct: Manifest[E]
                                          ) extends CustomSerializer[E](_ =>
  ( {
    case JString(name) =>
      Enum.valueOf(ct.runtimeClass.asInstanceOf[Class[E]], name)
  }, {
    case dt: E =>
      JString(dt.name())
  }
  )
)
