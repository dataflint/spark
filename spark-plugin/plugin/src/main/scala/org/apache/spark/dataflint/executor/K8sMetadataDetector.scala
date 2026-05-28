package org.apache.spark.dataflint.executor

import java.io.{FileInputStream, InputStream}
import java.util
import scala.util.Try

object K8sMetadataDetector {

  def isRunningInK8s: Boolean = sys.env.contains("KUBERNETES_SERVICE_HOST")

  def detect(extraConf: util.Map[String, String]): CloudMetadataDetector.CloudMetadata =
    detectWithLookups(extraConf, sys.env.get, loadPropertiesFromFile)

  // Two-source lookup so customers can pick whatever surfaces node identity into the pod:
  //   - env var (per field): easiest when node labels are mirrored onto pod labels (Karpenter
  //     mutating webhook, direct `spark.kubernetes.executor.env.*=...`, downward API fieldRef)
  //   - properties file: meant for the init-container pattern, where a pre-start container
  //     queries the K8s API for the pod's node and writes a `key=value` file into a shared
  //     volume. Env vars are immutable after pod creation, so a file is the only way to
  //     surface dynamically-discovered values into the executor JVM.
  // The existing `*.envVar` configs name the lookup key for BOTH sources: env var (process
  // env) and key inside the props file. Env var wins per-field when both are set; the file
  // fills in whatever's missing.
  private[executor] def detectWithLookups(
    extraConf: util.Map[String, String],
    envLookup: String => Option[String],
    fileLoader: String => util.Properties
  ): CloudMetadataDetector.CloudMetadata = {
    val fileProps: util.Properties =
      Option(extraConf.get("k8s.file"))
        .filter(_.nonEmpty)
        .map(fileLoader)
        .getOrElse(new util.Properties())

    // The `*.envVar` config value is the lookup key — same string is consulted against the
    // process env and the props file, so init-container output and direct env injection use
    // identical naming.
    def field(envConfKey: String): Option[String] = {
      val lookupKey = Option(extraConf.get(envConfKey)).filter(_.nonEmpty)
      lookupKey.flatMap(envLookup).filter(_.nonEmpty)
        .orElse(lookupKey.map(fileProps.getProperty).map(Option(_)).flatten.map(_.trim).filter(_.nonEmpty))
    }

    CloudMetadataDetector.CloudMetadata(
      cloudProvider = field("k8s.cloudProvider.envVar"),
      instanceType  = field("k8s.instanceType.envVar"),
      lifecycleType = field("k8s.lifecycleType.envVar"),
      region        = field("k8s.region.envVar")
    )
  }

  // Best-effort load — a missing/unreadable file is treated as "no data", never an error.
  private def loadPropertiesFromFile(path: String): util.Properties = {
    val props = new util.Properties()
    Try {
      val in: InputStream = new FileInputStream(path)
      try props.load(in) finally in.close()
    }
    props
  }
}