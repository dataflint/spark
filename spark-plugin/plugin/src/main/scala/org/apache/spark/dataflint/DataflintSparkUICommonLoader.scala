package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.api.DataflintPageFactory
import org.apache.spark.dataflint.listener.{DataflintDatabricksLiveListener, DataflintEnvironmentInfo, DataflintEnvironmentInfoEvent, DataflintListener, DataflintStore, DeltaLakeInstrumentationListener}
import org.apache.spark.dataflint.iceberg.ClassLoaderChecker
import org.apache.spark.dataflint.iceberg.ClassLoaderChecker.isMetricLoaderInRightClassLoader
import org.apache.spark.dataflint.saas.DataflintRunExporterListener
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.status.{ElementTrackingStore, LiveRDDsListener}
import org.apache.spark.ui.SparkUI

class DataflintSparkUICommonInstaller extends Logging {
  def install(context: SparkContext, pageFactory: DataflintPageFactory): String = {
    if(context.ui.isEmpty) {
      logWarning("No UI detected, skipping installation...")
      return ""
    }
    val isDataFlintAlreadyInstalled = pageFactory.getTabs(context.ui.get).exists(_.name == "DataFlint")
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
    val deltaLakeInstrumentationEnabled = context.conf.getBoolean("spark.dataflint.instrument.deltalake.enabled", defaultValue = false)
    val deltaLakeCollectZindexFields = context.conf.getBoolean("spark.dataflint.instrument.deltalake.collectZindexFields", defaultValue = false)
    val deltaLakeCacheZindexFieldsToProperties = context.conf.getBoolean("spark.dataflint.instrument.deltalake.cacheZindexFieldsToProperties", defaultValue = true)
    val deltaLakeHistoryLimit = context.conf.getInt("spark.dataflint.instrument.deltalake.historyLimit", defaultValue = 1000)
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
      if(deltaLakeInstrumentationEnabled) {
        val deltaLakeListener = new DeltaLakeInstrumentationListener(context, deltaLakeCollectZindexFields, deltaLakeCacheZindexFieldsToProperties, deltaLakeHistoryLimit, isDatabricks)
        addToQueueMethod(deltaLakeListener, "dataflint")
        logInfo("Added DeltaLakeInstrumentationListener to the listener bus")
      }
      context.listenerBus.post(DataflintEnvironmentInfoEvent(environmentInfo))
      if (isDatabricks) {
        addToQueueMethod(DataflintDatabricksLiveListener(context.listenerBus), "dataflint")
      }
    } catch {
      case e: Throwable =>
        logWarning("Could not add DataFlint Listeners to listener bus", e)
    }

    loadUI(context.ui.get, pageFactory, sqlListener)
  }

  def loadUI(ui: SparkUI, pageFactory: DataflintPageFactory, sqlListener: () => Option[SQLAppStatusListener] = () => None): String = {
    val isDataFlintAlreadyInstalled = pageFactory.getTabs(ui).exists(_.name == "DataFlint")
    if (isDataFlintAlreadyInstalled) {
      logInfo("DataFlint UI is already installed, skipping installation...")
      return ui.webUrl
    }
    pageFactory.addStaticHandler(ui, "io/dataflint/spark/static/ui", ui.basePath + "/dataflint")
    val dataflintStore = new DataflintStore(store = ui.store.store)
    val tab = pageFactory.createDataFlintTab(ui)
    tab.attachPage(pageFactory.createSQLPlanPage(ui, dataflintStore, sqlListener))
    tab.attachPage(pageFactory.createSQLMetricsPage(ui, sqlListener))
    tab.attachPage(pageFactory.createSQLStagesRddPage(ui))
    tab.attachPage(pageFactory.createApplicationInfoPage(ui, dataflintStore))
    tab.attachPage(pageFactory.createIcebergPage(ui, dataflintStore))
    tab.attachPage(pageFactory.createDeltaLakeScanPage(ui, dataflintStore))
    tab.attachPage(pageFactory.createCachedStoragePage(ui, dataflintStore))
    ui.attachTab(tab)
    ui.webUrl
  }

}

object DataflintSparkUICommonLoader extends Logging {

  private val DATAFLINT_EXTENSION_CLASS = "org.apache.spark.dataflint.DataFlintInstrumentationExtension"
  val INSTRUMENT_SPARK_ENABLED = "spark.dataflint.instrument.spark.enabled"
  val INSTRUMENT_MAP_IN_PANDAS_ENABLED = "spark.dataflint.instrument.spark.mapInPandas.enabled"
  val INSTRUMENT_MAP_IN_ARROW_ENABLED = "spark.dataflint.instrument.spark.mapInArrow.enabled"

  def install(context: SparkContext, pageFactory: DataflintPageFactory): String = {
    new DataflintSparkUICommonInstaller().install(context, pageFactory)
  }

  def loadUI(ui: SparkUI, pageFactory: DataflintPageFactory): String = {
    new DataflintSparkUICommonInstaller().loadUI(ui, pageFactory)
  }
  
  // Backward compatibility methods - these will be overridden in version-specific implementations
  def install(context: SparkContext): String = {
    throw new UnsupportedOperationException("This method requires a version-specific implementation. Use pluginspark3 or pluginspark4.")
  }

  def loadUI(ui: SparkUI): String = {
    throw new UnsupportedOperationException("This method requires a version-specific implementation. Use pluginspark3 or pluginspark4.")
  }

  /**
   * Registers the DataFlint instrumentation extension in spark.sql.extensions if not already present.
   * This must be called during plugin init() because spark.sql.extensions is read when SparkSession
   * is created, which occurs after plugin initialization.
   *
   * This method is in the org.apache.spark.dataflint package to access SparkContext.conf
   * (which is private[spark]).
   */
  def registerInstrumentationExtension(sc: SparkContext): Unit = {
    val instrumentEnabled = sc.conf.getBoolean(INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val mapInPandasEnabled = sc.conf.getBoolean(INSTRUMENT_MAP_IN_PANDAS_ENABLED, defaultValue = false)
    val mapInArrowEnabled = sc.conf.getBoolean(INSTRUMENT_MAP_IN_ARROW_ENABLED, defaultValue = false)
    val anyInstrumentationEnabled = instrumentEnabled || mapInPandasEnabled || mapInArrowEnabled
    if (!anyInstrumentationEnabled) {
      logInfo("DataFlint instrumentation extension is disabled (no instrumentation flags enabled)")
      return
    }

    try {
      val currentExtensions = sc.conf.get("spark.sql.extensions", "")
      if (currentExtensions.contains(DATAFLINT_EXTENSION_CLASS)) {
        logInfo("DataFlint instrumentation extension is already registered in spark.sql.extensions")
      } else {
        val newExtensions = if (currentExtensions.isEmpty) {
          DATAFLINT_EXTENSION_CLASS
        } else {
          s"$currentExtensions,$DATAFLINT_EXTENSION_CLASS"
        }
        sc.conf.set("spark.sql.extensions", newExtensions)
        logInfo(s"Registered DataFlint instrumentation extension in spark.sql.extensions")
      }
    } catch {
      case e: Throwable =>
        logWarning("Could not register DataFlint instrumentation extension", e)
    }
  }
}
