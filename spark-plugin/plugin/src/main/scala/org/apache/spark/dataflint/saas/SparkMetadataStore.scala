package org.apache.spark.dataflint.saas

import org.apache.spark.status.api.v1

case class SparkMetadataMetrics(
                                 containerMemoryGb: Double,
                                 executorJvmMemoryGb: Double,
                                 totalInputBytes: Long,
                                 totalOutputBytes: Long,
                                 totalSpillBytes: Long,
                                 totalShuffleWriteBytes: Long,
                                 totalShuffleReadBytes: Long,
                                 totalCachedMemoryBytes: Long,
                                 totalCachedDiskBytes: Long,
                                 maxExecutorCachedMemoryUsagePercentage: Double,                            executorPeakMemoryBytes: Long,
                                 containerPeakMemoryBytes: Long,
                                 executorJvmMemoryUsage: Double,
                                 driverJvmPeakMemoryBytes: Long,
                                 driverJvmMemoryUsage: Double,
                                 containerMemoryUsage: Double,
                                 totalDCU: Double,
                                 coreHourUsage: Double,
                                 memoryGbHour: Double,
                                 isAnySqlQueryFailed: Boolean,
                                 taskErrorRate: Double,
                                 idleCoresRatio: Double,
                                 CoresWastedRatio: Double,
                                 executorsDurationMs: Long,
                                 driverDurationMs: Long,

                               )

case class SparkMetadataStore(version: String,
                              runId: String,
                              accessKey: String,
                              applicationInfo: v1.ApplicationInfo,
                              metrics: SparkMetadataMetrics,
                              conf: Map[String, String])
