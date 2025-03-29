package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.api.{DataFlintTab, DataflintApplicationInfoPage, DataflintCachedStoragePage, DataflintIcebergPage, DataflintJettyUtils, DataflintSQLMetricsPage, DataflintSQLPlanPage, DataflintSQLStagesRddPage}
import org.apache.spark.dataflint.listener.{DataflintEnvironmentInfo, DataflintEnvironmentInfoEvent}
import org.apache.spark.dataflint.iceberg.ClassLoaderChecker
import org.apache.spark.dataflint.iceberg.ClassLoaderChecker.isMetricLoaderInRightClassLoader
import org.apache.spark.dataflint.listener.{DataflintDatabricksLiveListener, DataflintListener, DataflintStore}
import org.apache.spark.dataflint.saas.DataflintRunExporterListener
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.status.{ElementTrackingStore, LiveRDDsListener}
import org.apache.spark.ui.SparkUI

class DataflintSparkUIInstaller extends Logging {
  def install(context: SparkContext): String = {
    if(context.ui.isEmpty) {
      logWarning("No UI detected, skipping installation...")
      return ""
    }
    val isDataFlintAlreadyInstalled = context.ui.get.getTabs.exists(_.name == "DataFlint")
    if(isDataFlintAlreadyInstalled){
      logInfo("DataFlint UI is already installed, skipping installation...")
      return context.ui.get.webUrl
    }

    val sqlListener = () => context.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]]
    val dataflintListener = new DataflintListener(context.statusStore.store.asInstanceOf[ElementTrackingStore])
    val tokenConf = context.conf.getOption("spark.dataflint.token")
    val dataflintEnabled = context.conf.getBoolean("spark.dataflint.enabled", true)
    if(tokenConf.isDefined) {
      if (!tokenConf.get.contains("-")) {
        logWarning("Dataflint token is not valid, please check your configuration")
      } else if (!dataflintEnabled) {
        logWarning("Dataflint is explicitly disabled although token is defined, if you wish to re-enable it please set spark.dataflint.enabled to true")
      }
      else {
        context.listenerBus.addToQueue(new DataflintRunExporterListener(context), "dataflint")
        logInfo("Added DataflintRunExporterListener to the listener bus")
      }
    }

    val runtime = Runtime.getRuntime
    val driverXmxBytes = runtime.maxMemory()
    val environmentInfo = DataflintEnvironmentInfo(driverXmxBytes)
    val isDatabricks = context.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined
    val icebergInstalled = context.conf.get("spark.sql.extensions", "").contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    val icebergEnabled = context.conf.getBoolean("spark.dataflint.iceberg.enabled", defaultValue = true)
    val cacheObservabilityEnabled = context.conf.getBoolean("spark.dataflint.cacheObservability.enabled", defaultValue = true)
    val icebergAuthCatalogDiscovery = context.conf.getBoolean("spark.dataflint.iceberg.autoCatalogDiscovery", defaultValue = false)
    if(icebergInstalled && icebergEnabled) {
      if(icebergAuthCatalogDiscovery && isMetricLoaderInRightClassLoader()) {
        context.conf.getAll.filter(_._1.startsWith("spark.sql.catalog")).filter(keyValue => keyValue._2 == "org.apache.iceberg.spark.SparkCatalog" || keyValue._2 == "org.apache.iceberg.spark.SparkSessionCatalog").foreach(keyValue => {
          val configName = s"${keyValue._1}.metrics-reporter-impl"
          context.conf.getOption(configName) match {
            case Some(currentConfig) => {
              if(currentConfig == "org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter") {
                logInfo(s"Metric reporter already exist in config: ${configName}, no need to set it with dataflint iceberg auto discovery")
              } else {
                logWarning(s"Different metric reporter already exist in config: ${configName}, cannot set metric reporter to DataflintIcebergMetricsReporter")
              }
            }
            case None => {
              if(icebergAuthCatalogDiscovery) {
                context.conf.set(configName, "org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter")
                logInfo(s"set ${configName} reporter to DataflintIcebergMetricsReporter")
              } else {
                logWarning(s"DataflintIcebergMetricsReporter is missing for iceberg catalog ${configName}, for dataflint iceberg observability set spark.dataflint.iceberg.autoCatalogDiscovery to true or set the metric reporter manually to org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter")
              }
            }
          }
        })
      }
    }
    try {
      val addToQueueMethod =
        if (isDatabricks) (listener: SparkListenerInterface, queue: String) =>
          context.listenerBus.getClass.getMethods.find(_.getName == "addToQueue").head.invoke(context.listenerBus, listener, queue, None)
            .asInstanceOf[Unit]
        else (listener: SparkListenerInterface, queue: String) => context.listenerBus.addToQueue(listener, queue)
      addToQueueMethod(dataflintListener, "dataflint")

      if(cacheObservabilityEnabled) {
        val rddListener = new LiveRDDsListener(context.statusStore.store.asInstanceOf[ElementTrackingStore])
        addToQueueMethod(rddListener, "dataflint")
      }
      context.listenerBus.post(DataflintEnvironmentInfoEvent(environmentInfo))
      if (isDatabricks) {
        addToQueueMethod(DataflintDatabricksLiveListener(context.listenerBus), "dataflint")
      }
    } catch {
      case e: Throwable =>
        logWarning("Could not add DataFlint Listeners to listener bus", e)
    }

    loadUI(context.ui.get, sqlListener)
  }

  def loadUI(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    val isDataFlintAlreadyInstalled = ui.getTabs.exists(_.name == "DataFlint")
    if (isDataFlintAlreadyInstalled) {
      logInfo("DataFlint UI is already installed, skipping installation...")
      return ui.webUrl
    }
    DataflintJettyUtils.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val dataflintStore = new DataflintStore(store = ui.store.store)
    val tab = new DataFlintTab(ui)
    tab.attachPage(new DataflintSQLPlanPage(ui, dataflintStore, sqlListener))
    tab.attachPage(new DataflintSQLMetricsPage(ui, sqlListener))
    tab.attachPage(new DataflintSQLStagesRddPage(ui))
    tab.attachPage(new DataflintApplicationInfoPage(ui, dataflintStore))
    tab.attachPage(new DataflintIcebergPage(ui, dataflintStore))
    tab.attachPage(new DataflintCachedStoragePage(ui,  dataflintStore))
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