package org.apache.spark.dataflint

import org.apache.spark.status.api.v1

case class SparkMetadataMetrics(
                                 containerMemoryGb: Double,
                                 totalInputBytes: Long,
                                 totalOutputBytes: Long,
                                 totalSpillBytes: Long,
                                 totalShuffleWriteBytes: Long,
                                 totalShuffleReadBytes: Long,
                                 executorPeakMemoryBytes: Long,
                                 totalDCU: Double,
                                 coreHourUsage: Double,
                                 memoryGbHour: Double,
                                 isAnySqlQueryFailed: Boolean,
                                 taskErrorRate: Double,
                                 CoresWastedRatio: Double
                               )

case class SparkMetadataStore(version: String,
                              runId: String,
                              applicationInfo: v1.ApplicationInfo,
                              metrics: SparkMetadataMetrics,
                              conf: Map[String, String])
