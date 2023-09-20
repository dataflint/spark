package org.apache.spark

import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

object SparkUILoader {
  def load(context: SparkContext): String = {
    loadUI(context.ui.get)
  }

  def loadUI(ui: SparkUI): String = {
    ui.addStaticHandler("com/anecdota/spark/static/ui", ui.basePath + "/devtool")
    val tab = new DevtoolTab(ui)
    ui.attachTab(tab)
    ui.webUrl
  }
}
