package org.apache.spark.dataflint.executor

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.listener.{DataflintExecutorMetadataEvent, DataflintExecutorMetadataInfo}

object DriverMetadataHelper {

  def isExecutorMetadataEnabled(sc: SparkContext): Boolean = {
    sc.conf.getBoolean("spark.dataflint.executor.metadata.enabled", defaultValue = false)
  }

  def k8sInstanceTypeEnvVar(sc: SparkContext): Option[String] =
    sc.conf.getOption("spark.dataflint.executor.metadata.k8s.instanceType.envVar")

  def k8sLifecycleTypeEnvVar(sc: SparkContext): Option[String] =
    sc.conf.getOption("spark.dataflint.executor.metadata.k8s.lifecycleType.envVar")

  def k8sCloudProviderEnvVar(sc: SparkContext): Option[String] =
    sc.conf.getOption("spark.dataflint.executor.metadata.k8s.cloudProvider.envVar")

  def postExecutorMetadataEvent(sc: SparkContext, info: DataflintExecutorMetadataInfo): Unit = {
    sc.listenerBus.post(DataflintExecutorMetadataEvent(info))
  }
}