package org.apache.spark.dataflint

import org.apache.spark.status.api.v1

case class SparkMetadataMetrics(
                                containerMemoryGb: Double,
                                totalInputBytes: Long,
                                executorPeakMemoryBytes: Long,
                                totalDCU: Double,
                                coreHourUsage: Double,
                                memoryGbHour: Double)

case class SparkMetadataStore(runId: String,
                              applicationInfo: v1.ApplicationInfo,
                              metrics: SparkMetadataMetrics,
                              conf: Map[String, String])
