package org.apache.spark.dataflint.executor

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

import java.util

class DataflintExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // java.lang.Boolean.parseBoolean returns false for any non-"true" input (including bad
    // values) — String#toBoolean throws on those and would crash executor plugin init
    // before the main try/catch below could fire.
    val enabled = Option(extraConf.get("executor.metadata.enabled")).exists(java.lang.Boolean.parseBoolean)
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
      val executorJvmMaxHeapBytes = Runtime.getRuntime.maxMemory()

      val (cloudMetadata, cloudError) = try {
        val md = if (K8sMetadataDetector.isRunningInK8s) {
          logInfo("Detected Kubernetes environment, reading metadata from configured env vars")
          val envMd = K8sMetadataDetector.detect(extraConf)
          // Fall back to IMDS when env vars didn't yield anything — covers K8s setups where
          // the pod can reach the host metadata endpoint (e.g. IMDSv2 hop limit ≥ 2 or
          // hostNetwork pods). Cheap no-op when the metadata endpoint isn't reachable.
          if (envMd.instanceType.isDefined) envMd
          else {
            logInfo("K8s env-var detection returned no instanceType; falling back to cloud metadata endpoint")
            CloudMetadataDetector.detect()
          }
        } else {
          CloudMetadataDetector.detect()
        }
        (md, None)
      } catch {
        case e: Throwable =>
          logWarning("Failed to detect cloud metadata", e)
          val reason = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          (CloudMetadataDetector.CloudMetadata(None, None, None, None), Some(s"cloud metadata detection failed: $reason"))
      }

      val message = ExecutorMetadataMessage(
        executorId = executorId,
        executorHost = hostname,
        instanceType = cloudMetadata.instanceType,
        lifecycleType = cloudMetadata.lifecycleType,
        cloudProvider = cloudMetadata.cloudProvider,
        region = cloudMetadata.region,
        osName = osName,
        osArch = osArch,
        jvmVersion = jvmVersion,
        availableProcessors = availableProcessors,
        executorJvmMaxHeapBytes = executorJvmMaxHeapBytes,
        collectionError = cloudError
      )
      ctx.send(message)
      logInfo(s"Sent executor metadata: provider=${cloudMetadata.cloudProvider.getOrElse("unknown")}, " +
        s"instance=${cloudMetadata.instanceType.getOrElse("unknown")}, " +
        s"lifecycle=${cloudMetadata.lifecycleType.getOrElse("unknown")}, " +
        s"region=${cloudMetadata.region.getOrElse("unknown")}")
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
            region = None,
            osName = System.getProperty("os.name", "unknown"),
            osArch = System.getProperty("os.arch", "unknown"),
            jvmVersion = System.getProperty("java.version", "unknown"),
            availableProcessors = Runtime.getRuntime.availableProcessors(),
            executorJvmMaxHeapBytes = Runtime.getRuntime.maxMemory(),
            collectionError = Some(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
          ))
        } catch {
          case inner: Throwable =>
            logWarning("Failed to send error metadata to driver", inner)
        }
    }
  }
}
