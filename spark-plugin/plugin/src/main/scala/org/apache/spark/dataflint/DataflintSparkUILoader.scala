package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.SparkUI

class DataflintSparkUIInstaller extends Logging {
  def install(context: SparkContext): String = {
    val sqlListener = () => context.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]]
    val tokenConf = context.conf.getOption("spark.dataflint.token")
    val dataflintEnabled = context.conf.getBoolean("spark.dataflint.enabled", true)
    if(tokenConf.isDefined) {
      if (!tokenConf.get.contains("-")) {
        logWarning("Dataflint token is not valid, please check your configuration")
      } else if (!dataflintEnabled) {
        logWarning("Dataflint is explicitly disabled although token is defined, if you which to re-enable it please set spark.dataflint.enabled to true")
      }
      else {
        context.conf.set("spark.dataflint.runId", java.util.UUID.randomUUID.toString.replaceAll("-", ""))
        context.listenerBus.addToQueue(new DataflintListener(context), "dataflint")
      }
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
object DataflintSparkUILoader {
  def install(context: SparkContext): String = {
    new DataflintSparkUIInstaller().install(context)
  }

  def loadUI(ui: SparkUI): String = {
    new DataflintSparkUIInstaller().loadUI(ui)
  }
}
