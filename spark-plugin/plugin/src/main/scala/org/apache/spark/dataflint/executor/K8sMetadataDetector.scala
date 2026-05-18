package org.apache.spark.dataflint.executor

import java.util

object K8sMetadataDetector {

  def isRunningInK8s: Boolean = sys.env.contains("KUBERNETES_SERVICE_HOST")

  def detect(extraConf: util.Map[String, String]): CloudMetadataDetector.CloudMetadata =
    detectWithEnv(extraConf, sys.env.get)

  private[executor] def detectWithEnv(
    extraConf: util.Map[String, String],
    envLookup: String => Option[String]
  ): CloudMetadataDetector.CloudMetadata = {
    val instanceType = readFromEnvVar(extraConf, "k8s.instanceType.envVar", envLookup)
    val lifecycleType = readFromEnvVar(extraConf, "k8s.lifecycleType.envVar", envLookup)
    val cloudProvider = readFromEnvVar(extraConf, "k8s.cloudProvider.envVar", envLookup)
    CloudMetadataDetector.CloudMetadata(cloudProvider, instanceType, lifecycleType)
  }

  private def readFromEnvVar(
    extraConf: util.Map[String, String],
    configKey: String,
    envLookup: String => Option[String]
  ): Option[String] = {
    Option(extraConf.get(configKey))
      .filter(_.nonEmpty)
      .flatMap(envLookup)
      .filter(_.nonEmpty)
  }
}