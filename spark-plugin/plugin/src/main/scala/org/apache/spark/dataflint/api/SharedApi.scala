package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.{IcebergCommitInfo, DataflintEnvironmentInfo, DataflintDeltaLakeScanInfo}
import org.apache.spark.status.api.v1.ApplicationInfo

// Shared API case classes — used by both Spark 3 and Spark 4 modules

case class NodeMetric(name: String, value: Option[String])

case class DataFlintApplicationInfo(runId: Option[String], info: ApplicationInfo, environmentInfo: Option[DataflintEnvironmentInfo])

case class IcebergInfo(commitsInfo: Seq[IcebergCommitInfo])

case class DeltaLakeScanInfo(scans: Seq[DataflintDeltaLakeScanInfo])

case class NodeMetrics(id: Long, name: String, metrics: Seq[NodeMetric])

case class NodePlan(id: Long, planDescription: String, rddScopeId: Option[String])

// Duration attribution types
case class NodeDurationData(
  nodeId: Long,
  nodeName: String,
  durationMs: Long,
  // Exchange-specific: split into write (shuffle write time) and read (fetch wait time) durations
  writeDurationMs: Long = 0,
  readDurationMs: Long = 0
)

/** A resolved stage group with actual Spark stageId and stage metadata. */
case class ResolvedStageGroup(
  sparkStageId: Int,
  status: String,
  numTasks: Int,
  numCompleteTasks: Int,
  executorRunTime: Long,
  nodeIds: Seq[Long],
  exchangeWriteIds: Seq[Long],
  exchangeReadIds: Seq[Long]
)

/** Enriched sqlmetrics response: node metrics + duration attribution */
case class SQLMetricsWithDuration(
  nodeMetrics: Seq[NodeMetrics],
  stageGroups: Seq[ResolvedStageGroup],
  nodeDurations: Seq[NodeDurationData]
)