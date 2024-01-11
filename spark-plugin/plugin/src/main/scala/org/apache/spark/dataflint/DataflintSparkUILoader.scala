package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

object DataflintSparkUILoader {
  def install(context: SparkContext): String = {
    val sqlListener = () => context.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]]
    // this code that adds a listener that export the spark run is only activated if we are in SaaS mode (meaning spark.dataflint.token has value)
    // so in the default open-source mode nobody is going to export your spark data anywhere :)
    if(context.conf.getOption("spark.dataflint.token").isDefined) {
      context.conf.set("spark.dataflint.runId", java.util.UUID.randomUUID.toString.replaceAll("-", ""))
      context.listenerBus.addToQueue(new DataflintListener(context), "dataflint")
    }
    loadUI(context.ui.get, sqlListener)
  }

  def loadUI(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    DataflintJettyUtils.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val tab = new DataflintTab(ui)
    tab.attachPage(new DataflintSQLPlanPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLMetricsPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLStagesRddPage(ui))
    tab.attachPage(new DataflintApplicationInfoPage(ui))
    ui.attachTab(tab)
    ui.webUrl
  }
}
