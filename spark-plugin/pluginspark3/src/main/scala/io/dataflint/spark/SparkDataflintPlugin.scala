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
    // DriverPlugin.init failures abort SparkContext start, so any bug in our setup would
    // crash the user's Spark job. Swallow + log so DataFlint can never take down Spark.
    try {
      DataflintSparkUICommonLoader.recordPluginVersion(sc, classOf[SparkDataflintPlugin])
      DataflintSparkUICommonLoader.registerInstrumentationExtension(sc)
      val executorMetadataEnabled = DriverMetadataHelper.isExecutorMetadataEnabled(sc)
      val conf = scala.collection.mutable.Map[String, String](
        "executor.metadata.enabled" -> executorMetadataEnabled.toString
      )
      DriverMetadataHelper.k8sInstanceTypeEnvVar(sc).foreach(v => conf("k8s.instanceType.envVar") = v)
      DriverMetadataHelper.k8sLifecycleTypeEnvVar(sc).foreach(v => conf("k8s.lifecycleType.envVar") = v)
      DriverMetadataHelper.k8sCloudProviderEnvVar(sc).foreach(v => conf("k8s.cloudProvider.envVar") = v)
      DriverMetadataHelper.k8sRegionEnvVar(sc).foreach(v => conf("k8s.region.envVar") = v)
      DriverMetadataHelper.k8sPropsFile(sc).foreach(v => conf("k8s.file") = v)
      conf.toMap.asJava
    } catch {
      case e: Throwable =>
        logWarning("DataFlint driver plugin init failed; continuing without DataFlint instrumentation", e)
        java.util.Collections.emptyMap[String, String]()
    }
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
            region = msg.region,
            osName = msg.osName,
            osArch = msg.osArch,
            jvmVersion = msg.jvmVersion,
            availableProcessors = msg.availableProcessors,
            executorJvmMaxHeapBytes = msg.executorJvmMaxHeapBytes,
            collectionError = msg.collectionError
          )
          DriverMetadataHelper.postExecutorMetadataEvent(sc, info)
          logInfo(s"Received executor metadata from executor ${msg.executorId}: " +
            s"provider=${msg.cloudProvider.getOrElse("unknown")}, " +
            s"instance=${msg.instanceType.getOrElse("unknown")}, " +
            s"lifecycle=${msg.lifecycleType.getOrElse("unknown")}, " +
            s"region=${msg.region.getOrElse("unknown")}")
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
