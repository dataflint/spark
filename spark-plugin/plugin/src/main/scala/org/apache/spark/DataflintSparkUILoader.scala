package org.apache.spark

import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

object DataflintSparkUILoader {
  def install(context: SparkContext): String = {
    val sqlListener = () => context.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]]
    loadUI(context.ui.get, sqlListener)
  }

  def loadUI(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    DataflintJettyUtils.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val tab = new DataflintTab(ui)
    tab.attachPage(new DataflintSQLPlanPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLMetricsPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLStagesRddPage(ui))
    ui.attachTab(tab)
    ui.webUrl
  }
}
