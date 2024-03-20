package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.api.{DataFlintTab, DataflintApplicationInfoPage, DataflintIcebergPage, DataflintJettyUtils, DataflintSQLMetricsPage, DataflintSQLPlanPage, DataflintSQLStagesRddPage}
import org.apache.spark.dataflint.listener.{DataflintListener, DataflintStore}
import org.apache.spark.dataflint.saas.DataflintRunExporterListener
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.status.ElementTrackingStore
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
        context.listenerBus.addToQueue(new DataflintRunExporterListener(context), "dataflint")
      }
    }

    // DataflintListener currently only relevant for iceberg support, so no need to add the listener if iceberg support is off
    val icebergInstalled = context.conf.get("spark.sql.extensions", "").contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    val icebergEnabled = context.conf.getBoolean("spark.dataflint.iceberg.enabled", defaultValue = true)
    if(icebergInstalled && icebergEnabled) {
      context.listenerBus.addToQueue(new DataflintListener(context.statusStore.store.asInstanceOf[ElementTrackingStore]), "dataflint")
    }
    loadUI(context.ui.get, sqlListener)
  }

  def loadUI(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    DataflintJettyUtils.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val dataflintStore = new DataflintStore(store = ui.store.store)
    val tab = new DataFlintTab(ui)
    tab.attachPage(new DataflintSQLPlanPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLMetricsPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLStagesRddPage(ui))
    tab.attachPage(new DataflintApplicationInfoPage(ui))
    tab.attachPage(new DataflintIcebergPage(ui, dataflintStore))
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
