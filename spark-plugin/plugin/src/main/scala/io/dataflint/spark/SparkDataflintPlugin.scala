package io.dataflint.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.dataflint.DataflintSparkUILoader
import org.apache.spark.internal.Logging

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class SparkDataflintPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new SparkDataflintDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = null
}

class SparkDataflintDriverPlugin extends DriverPlugin with Logging {
  var sc: SparkContext = null

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    this.sc = sc
    Map[String, String]().asJava
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    var webUrl = DataflintSparkUILoader.install(sc)
    logInfo(s"spark dataflint url is $webUrl/dataflint")
    super.registerMetrics(appId, pluginContext)
  }
}
