package org.apache.spark

import org.apache.spark.ui.SparkUI

object SparkUILoader {
  def load(context: SparkContext): String = {
    loadUI(context.ui.get)
  }

  def loadUI(ui: SparkUI): String = {
    ui.addStaticHandler("com/anecdota/spark/static/ui", ui.basePath + "/devtool")
    ui.attachTab(new DevtoolTab(ui))
    ui.webUrl
  }
}
