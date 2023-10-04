package org.apache.spark

import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

object DataflintSparkUILoader {
  def load(context: SparkContext): String = {
    loadUI(context.ui.get)
  }

  def loadUI(ui: SparkUI): String = {
    DataflintJettyUtils.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val tab = new DataflintTab(ui)
    ui.attachTab(tab)
    ui.webUrl
  }
}
