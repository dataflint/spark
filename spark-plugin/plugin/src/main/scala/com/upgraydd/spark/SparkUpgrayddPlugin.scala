package com.upgraydd.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class SparkUpgrayddPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new SparkUpgreyddDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = null
}

class SparkUpgreyddDriverPlugin extends DriverPlugin {
  var sc: SparkContext = null

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    this.sc = sc
    Map[String, String]().asJava
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    SparkUpgraydd.upgrade(sc)
    super.registerMetrics(appId, pluginContext)

  }
}
