package org.apache.spark.dataflint.api

import org.apache.spark.ui.{SparkUI, WebUITab}

class DataFlintTab(parent: SparkUI) extends WebUITab(parent, "dataflint") {
  override val name: String = "DataFlint"
}