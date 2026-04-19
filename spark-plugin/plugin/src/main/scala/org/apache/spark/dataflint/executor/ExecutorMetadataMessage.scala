package org.apache.spark.dataflint.executor

case class ExecutorMetadataMessage(
  executorId: String,
  executorHost: String,
  instanceType: Option[String],
  lifecycleType: Option[String],
  cloudProvider: Option[String],
  osName: String,
  osArch: String,
  jvmVersion: String,
  availableProcessors: Int,
  totalMemoryBytes: Long,
  collectionError: Option[String]
) extends java.io.Serializable
