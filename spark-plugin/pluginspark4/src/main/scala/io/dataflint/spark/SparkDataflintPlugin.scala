package io.dataflint.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.dataflint.{DataflintSparkUICommonLoader, DataflintSparkUILoader}
import org.apache.spark.dataflint.executor.{DataflintExecutorPlugin, DriverMetadataHelper, ExecutorMetadataMessage}
import org.apache.spark.dataflint.listener.DataflintExecutorMetadataInfo
import org.apache.spark.internal.Logging

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class SparkDataflintPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new SparkDataflintDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = new DataflintExecutorPlugin()
}

class SparkDataflintDriverPlugin extends DriverPlugin with Logging {
  var sc: SparkContext = null

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    this.sc = sc
    DataflintSparkUICommonLoader.registerInstrumentationExtension(sc)
    val executorMetadataEnabled = DriverMetadataHelper.isExecutorMetadataEnabled(sc)
    Map("executor.metadata.enabled" -> executorMetadataEnabled.toString).asJava
  }

  override def receive(message: Any): String = {
    message match {
      case msg: ExecutorMetadataMessage =>
        try {
          val info = DataflintExecutorMetadataInfo(
            executorId = msg.executorId,
            executorHost = msg.executorHost,
            instanceType = msg.instanceType,
            lifecycleType = msg.lifecycleType,
            cloudProvider = msg.cloudProvider,
            osName = msg.osName,
            osArch = msg.osArch,
            jvmVersion = msg.jvmVersion,
            availableProcessors = msg.availableProcessors,
            totalMemoryBytes = msg.totalMemoryBytes,
            collectionError = msg.collectionError
          )
          DriverMetadataHelper.postExecutorMetadataEvent(sc, info)
          logInfo(s"Received executor metadata from executor ${msg.executorId}: " +
            s"provider=${msg.cloudProvider}, instance=${msg.instanceType}, lifecycle=${msg.lifecycleType}")
          null
        } catch {
          case e: Throwable =>
            logWarning(s"Failed to process executor metadata from ${msg.executorId}", e)
            null
        }
      case _ =>
        logWarning(s"Received unknown message type: ${message.getClass.getName}")
        null
    }
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    var webUrl = DataflintSparkUILoader.install(sc)
    logInfo(s"spark dataflint url is $webUrl/dataflint")
    super.registerMetrics(appId, pluginContext)
  }
}
