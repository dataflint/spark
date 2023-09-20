package org.apache.spark

import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

object SparkUILoader {
  def load(context: SparkContext): String = {
    val sqlListener = () => context.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]]
    loadUI(context.ui.get, sqlListener)
  }

  def loadUI(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    ui.addStaticHandler("com/anecdota/spark/static/ui", ui.basePath + "/devtool")
    val tab = new DevtoolTab(ui)
    tab.attachPage(new SqlLiveMetricPage(ui, sqlListener))
    ui.attachTab(tab)
    ui.webUrl
  }
}
