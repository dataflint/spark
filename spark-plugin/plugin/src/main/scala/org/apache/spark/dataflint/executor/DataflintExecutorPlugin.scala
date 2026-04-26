package org.apache.spark.dataflint.executor

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

import java.util

class DataflintExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    val enabled = Option(extraConf.get("executor.metadata.enabled")).exists(_.toBoolean)
    if (!enabled) {
      return
    }

    val executorId = ctx.executorID()
    val hostname = try {
      java.net.InetAddress.getLocalHost.getHostName
    } catch {
      case _: Throwable => "unknown"
    }

    try {
      val osName = System.getProperty("os.name", "unknown")
      val osArch = System.getProperty("os.arch", "unknown")
      val jvmVersion = System.getProperty("java.version", "unknown")
      val availableProcessors = Runtime.getRuntime.availableProcessors()
      val totalMemoryBytes = Runtime.getRuntime.maxMemory()

      val cloudMetadata = try {
        CloudMetadataDetector.detect()
      } catch {
        case e: Throwable =>
          logWarning("Failed to detect cloud metadata", e)
          CloudMetadataDetector.CloudMetadata(None, None, None)
      }

      val message = ExecutorMetadataMessage(
        executorId = executorId,
        executorHost = hostname,
        instanceType = cloudMetadata.instanceType,
        lifecycleType = cloudMetadata.lifecycleType,
        cloudProvider = cloudMetadata.cloudProvider,
        osName = osName,
        osArch = osArch,
        jvmVersion = jvmVersion,
        availableProcessors = availableProcessors,
        totalMemoryBytes = totalMemoryBytes,
        collectionError = None
      )
      ctx.send(message)
      logInfo(s"Sent executor metadata: provider=${cloudMetadata.cloudProvider}, " +
        s"instance=${cloudMetadata.instanceType}, lifecycle=${cloudMetadata.lifecycleType}")
    } catch {
      case e: Throwable =>
        logWarning("Failed to collect/send executor metadata", e)
        try {
          ctx.send(ExecutorMetadataMessage(
            executorId = executorId,
            executorHost = hostname,
            instanceType = None,
            lifecycleType = None,
            cloudProvider = None,
            osName = System.getProperty("os.name", "unknown"),
            osArch = System.getProperty("os.arch", "unknown"),
            jvmVersion = System.getProperty("java.version", "unknown"),
            availableProcessors = Runtime.getRuntime.availableProcessors(),
            totalMemoryBytes = Runtime.getRuntime.maxMemory(),
            collectionError = Some(e.getMessage)
          ))
        } catch {
          case inner: Throwable =>
            logWarning("Failed to send error metadata to driver", inner)
        }
    }
  }
}
