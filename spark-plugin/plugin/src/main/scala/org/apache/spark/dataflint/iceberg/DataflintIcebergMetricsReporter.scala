package org.apache.spark.dataflint.iceberg

import org.apache.iceberg.metrics.{CommitMetricsResult, CommitReport, MetricsReport, MetricsReporter}
import org.apache.spark.dataflint.listener.{IcebergCommitEvent, IcebergCommitInfo, IcebergCommitMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class DataflintIcebergMetricsReporter extends MetricsReporter with Logging {
  private def exportMetrics(metrics: CommitMetricsResult): IcebergCommitMetrics = {
    IcebergCommitMetrics(
      durationMS = Option(metrics.totalDuration()).map(_.totalDuration().toMillis).getOrElse(0L),
      attempts = Option(metrics.attempts()).map(_.value()).getOrElse(0L),
      addedDataFiles = Option(metrics.addedDataFiles()).map(_.value()).getOrElse(0L),
      removedDataFiles = Option(metrics.removedDataFiles()).map(_.value()).getOrElse(0L),
      totalDataFiles = Option(metrics.totalDataFiles()).map(_.value()).getOrElse(0L),
      addedDeleteFiles = Option(metrics.addedDeleteFiles()).map(_.value()).getOrElse(0L),
      addedEqualityDeleteFiles = Option(metrics.addedEqualityDeleteFiles()).map(_.value()).getOrElse(0L),
      addedPositionalDeleteFiles = Option(metrics.addedPositionalDeleteFiles()).map(_.value()).getOrElse(0L),
      removedDeleteFiles = Option(metrics.removedDeleteFiles()).map(_.value()).getOrElse(0L),
      removedEqualityDeleteFiles = Option(metrics.removedEqualityDeleteFiles()).map(_.value()).getOrElse(0L),
      removedPositionalDeleteFiles = Option(metrics.removedPositionalDeleteFiles()).map(_.value()).getOrElse(0L),
      totalDeleteFiles = Option(metrics.totalDeleteFiles()).map(_.value()).getOrElse(0L),
      addedRecords = Option(metrics.addedRecords()).map(_.value()).getOrElse(0L),
      removedRecords = Option(metrics.removedRecords()).map(_.value()).getOrElse(0L),
      totalRecords = Option(metrics.totalRecords()).map(_.value()).getOrElse(0L),
      addedFilesSizeInBytes = Option(metrics.addedFilesSizeInBytes()).map(_.value()).getOrElse(0L),
      removedFilesSizeInBytes = Option(metrics.removedFilesSizeInBytes()).map(_.value()).getOrElse(0L),
      totalFilesSizeInBytes = Option(metrics.totalFilesSizeInBytes()).map(_.value()).getOrElse(0L),
      addedPositionalDeletes = Option(metrics.addedPositionalDeletes()).map(_.value()).getOrElse(0L),
      removedPositionalDeletes = Option(metrics.removedPositionalDeletes()).map(_.value()).getOrElse(0L),
      totalPositionalDeletes = Option(metrics.totalPositionalDeletes()).map(_.value()).getOrElse(0L),
      addedEqualityDeletes = Option(metrics.addedEqualityDeletes()).map(_.value()).getOrElse(0L),
      removedEqualityDeletes = Option(metrics.removedEqualityDeletes()).map(_.value()).getOrElse(0L),
      totalEqualityDeletes = Option(metrics.totalEqualityDeletes()).map(_.value()).getOrElse(0L)
    )
  }

  override def report(metricsReport: MetricsReport): Unit = {
    try {
      metricsReport match {
        case commitMetrics: CommitReport => {
          SparkSession.getActiveSession.foreach(session => {
            val context = session.sparkContext
            val executionId = context.getLocalProperty("spark.sql.execution.id")
            val metrics = commitMetrics.commitMetrics()
            commitMetrics.operation()
            val info = IcebergCommitInfo(
              executionId = executionId.toInt,
              tableName = commitMetrics.tableName(),
              commitId = commitMetrics.sequenceNumber(),
              operation = commitMetrics.operation(),
              metrics = exportMetrics(metrics)
            )
            context.listenerBus.post(IcebergCommitEvent(info))
          })
        }
        case _ => {}
      }
    } catch {
      case e: Exception => logError("Error while reporting iceberg metrics", e)
    }
  }
}
