package org.apache.spark

package object dataflint {
  private[dataflint] type EnumValue[A <: Enumeration] = A#Value
}
