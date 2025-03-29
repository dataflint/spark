package org.apache.spark.dataflint.listener

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.kvstore.KVIndex

case class DatabricksAdditionalExecutionInfo(
                       executionId: Long,
                       version: Long,
                       nodeIdToRddScopeId: Map[Long, String]) {
}

case class DatabricksAdditionalExecutionEvent(databricksAdditionalExecutionInfo: DatabricksAdditionalExecutionInfo) extends SparkListenerEvent

class DatabricksAdditionalExecutionWrapper(val info: DatabricksAdditionalExecutionInfo) {
  @KVIndex
  @JsonIgnore
  def id: Long = info.executionId
}

case class IcebergCommitMetrics(
                                 durationMS: Long,
                                 attempts: Long,
                                 addedDataFiles: Long,
                                 removedDataFiles: Long,
                                 totalDataFiles: Long,
                                 addedDeleteFiles: Long,
                                 addedEqualityDeleteFiles: Long,
                                 addedPositionalDeleteFiles: Long,
                                 removedDeleteFiles: Long,
                                 removedEqualityDeleteFiles: Long,
                                 removedPositionalDeleteFiles: Long,
                                 totalDeleteFiles: Long,
                                 addedRecords: Long,
                                 removedRecords: Long,
                                 totalRecords: Long,
                                 addedFilesSizeInBytes: Long,
                                 removedFilesSizeInBytes: Long,
                                 totalFilesSizeInBytes: Long,
                                 addedPositionalDeletes: Long,
                                 removedPositionalDeletes: Long,
                                 totalPositionalDeletes: Long,
                                 addedEqualityDeletes: Long,
                                 removedEqualityDeletes: Long,
                                 totalEqualityDeletes: Long
                               )

case class IcebergCommitInfo(executionId: Int, tableName: String, commitId: Long, operation: String, metrics: IcebergCommitMetrics )

case class IcebergCommitEvent(icebergCommit: IcebergCommitInfo) extends SparkListenerEvent

class IcebergCommitWrapper(val info: IcebergCommitInfo) {

  @JsonIgnore @KVIndex
  def id: String = info.executionId.toString
}

case class DataflintEnvironmentInfo(driverXmxBytes: Long)

case class DataflintEnvironmentInfoEvent(environmentInfo: DataflintEnvironmentInfo) extends SparkListenerEvent

class DataflintEnvironmentInfoWrapper(val info: DataflintEnvironmentInfo) {
  @KVIndex
  @JsonIgnore
  def id: String = "environment_info"
}

case class DataflintExecutorStorageInfo(memoryUsed: Long, memoryRemaining: Long, memoryUsagePercentage: Double)

case class DataflintRDDStorageInfo(rddId: Int, memoryUsed: Long, diskUsed: Long, numOfPartitions: Int, storageLevel: String, maxMemoryExecutorInfo: Option[DataflintExecutorStorageInfo])

class DataflintRDDStorageInfoWrapper(val info: DataflintRDDStorageInfo) {
  @KVIndex
  def id: Int = info.rddId
}
