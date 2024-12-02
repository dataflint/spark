package org.apache.spark.dataflint.jobgroup

import org.apache.spark.SparkConf
import org.apache.spark.dataflint.listener.DatabricksAdditionalExecutionWrapper
import org.apache.spark.dataflint.saas.StageTaskSummary
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.sql.execution.ui.{SQLAppStatusStore, SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.status.api.v1.{ExecutorStageSummary, ExecutorSummary}
import org.apache.spark.status.{ExecutorStageSummaryWrapper, _}
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

import java.util.Date
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.reflect.{ClassTag, classTag}


class JobGroupExtractor(store: AppStatusStore, sqlStore: SQLAppStatusStore) {
  private val kvStore = store.store.asInstanceOf[ElementTrackingStore]


  def getGroupList(): Seq[String] = {
    val allJobs = store.jobsList(null)
    allJobs.flatMap(job => job.jobGroup).distinct
  }

  def extract(jobGroupId: String): (AppStatusStore, SQLAppStatusStore, Long) = {
    val allJobs = store.jobsList(null)
    val jobs = allJobs.filter(job => job.jobGroup.contains(jobGroupId))
    val startTime = jobs.flatMap(_.submissionTime).min
    val endTime = jobs.flatMap(_.completionTime).min

    val jobsIds = jobs.map(_.jobId)
    val stagesIds = jobs.flatMap(_.stageIds)
    val stages = store.stageList(null).filter(stage => stagesIds.contains(stage.stageId))
    val tasksIds = stages.flatMap(_.tasks.map(_.keys.toSeq))
    val sqlsIds = sqlStore.executionsList().filter(_.jobs.keys.toSeq.exists(jobsIds.contains)).map(_.executionId)
    val rddIds = stages.flatMap(_.rddIds).distinct
    val stageAndAttemptIds = stages.map(stage => Array(stage.stageId, stage.attemptId))
    val stageIds = stages.map(stage => stage.stageId).distinct

    val newStore = new InMemoryStore()
    val conf = new SparkConf().set(ASYNC_TRACKING_ENABLED, false)
    val newTrackingStore = new ElementTrackingStore(newStore, conf)

    // Filter the list of executors to include only those within the given start and end times.
    val executorsInJob = readAll[ExecutorSummaryWrapper].filter { executor =>
      // Get the executor's removeTime in milliseconds, or use Long.MaxValue if removeTime is None.
      val executorRemoveTime = executor.info.removeTime.map(_.getTime).getOrElse(Long.MaxValue)

      // Get the executor's addTime in milliseconds.
      val executorAddTime = executor.info.addTime.getTime

      // Include the executor if:
      // 1. Its removeTime is greater than or equal to the start time, meaning it is active during the period.
      // 2. Its addTime is less than or equal to the end time, meaning it was added before or at the end of the period.
      executorRemoveTime >= startTime.getTime && executorAddTime <= endTime.getTime
    }

    val executorIds = executorsInJob.map(_.info.id)

    val applicationInfo = readAll[ApplicationInfoWrapper].head

    writeAll(newTrackingStore, readAll[ApplicationEnvironmentInfoWrapper])
    writeAll(newTrackingStore, readAll[ResourceProfileWrapper])

    writeAll(newTrackingStore, Seq(new AppSummary(jobsIds.length, stageAndAttemptIds.length)))
    writeAll(newTrackingStore, Seq(new ApplicationInfoWrapper(
      applicationInfo.info.copy(attempts =
        applicationInfo.info.attempts.map(_.copy(
          startTime = startTime,
          endTime = endTime,
          lastUpdated = endTime,
          duration = endTime.getTime - startTime.getTime,
          completed = true)))
    )))

    val executorStageSummaryWrapper = readAll[ExecutorStageSummaryWrapper]
      .filter(executorStageSummaryWrapper => stageAndAttemptIds.exists(stageAndAttemptId =>
        executorStageSummaryWrapper.stageId == stageAndAttemptId(0) &&
          executorStageSummaryWrapper.stageAttemptId == stageAndAttemptId(1)
      ))

    writeAll(newTrackingStore, executorStageSummaryWrapper)
    writeAll(newTrackingStore, rebuildExecutorSummaries(executorsInJob, executorStageSummaryWrapper, startTime, endTime))

    writeAll(newTrackingStore, readAll[StreamBlockData].filter(block => executorIds.contains(block.executorId)))


    writeAll(newTrackingStore, jobsIds.map(jobId => kvStore.read(classOf[JobDataWrapper], jobId)))

    writeAll(newTrackingStore, stageAndAttemptIds.map(stageAndAttemptId => kvStore.read(classOf[StageDataWrapper], stageAndAttemptId)))
    writeAll(newTrackingStore, readAllIfExists[ExecutorStageSummaryWrapper](stageAndAttemptIds))
    writeAll(newTrackingStore, readAllIfExists[StageTaskSummary](stageAndAttemptIds))

    writeAll(newTrackingStore, readAllIfExists[RDDStorageInfoWrapper](rddIds.asInstanceOf[Seq[Object]]))

    writeAll(newTrackingStore, tasksIds.map(taskId => kvStore.read(classOf[TaskDataWrapper], taskId)))

    writeAll(newTrackingStore, stageIds.map(stageId => kvStore.read(classOf[RDDOperationGraphWrapper], stageId)))
    writeAll(newTrackingStore, readAll[PoolData].filter(_.stageIds.exists(stageIds.contains)))

    writeAll(newTrackingStore, sqlsIds.map(sqlId => kvStore.read(classOf[SQLExecutionUIData], sqlId)))
    writeAll(newTrackingStore, sqlsIds.map(sqlId => kvStore.read(classOf[SparkPlanGraphWrapper], sqlId)))
    writeAll(newTrackingStore, readAllIfExists[DatabricksAdditionalExecutionWrapper](sqlsIds.asInstanceOf[Seq[Object]]))

    (new AppStatusStore(newTrackingStore), new SQLAppStatusStore(newTrackingStore), endTime.toInstant.toEpochMilli)
  }

  def rebuildExecutorSummaries(
                                executorsInJob: Seq[ExecutorSummaryWrapper],
                                executorStageSummaries: Seq[ExecutorStageSummaryWrapper],
                                startTime: Date,
                                endTime: Date
                              ): Seq[ExecutorSummaryWrapper] = {

    // Helper method to sum metrics for a given executor ID
    def sumMetricsForExecutor(executorId: String): ExecutorStageSummary = {
      // Filter stage summaries by executor ID
      val filteredMetrics = executorStageSummaries.filter(_.executorId == executorId)

      // Aggregate metrics for the executor
      filteredMetrics.foldLeft(
        new ExecutorStageSummary(
          taskTime = 0L,
          failedTasks = 0,
          succeededTasks = 0,
          killedTasks = 0,
          inputBytes = 0L,
          inputRecords = 0L,
          outputBytes = 0L,
          outputRecords = 0L,
          shuffleRead = 0L,
          shuffleReadRecords = 0L,
          shuffleWrite = 0L,
          shuffleWriteRecords = 0L,
          memoryBytesSpilled = 0L,
          diskBytesSpilled = 0L,
          isBlacklistedForStage = false,
          peakMemoryMetrics = None,
          isExcludedForStage = false
        )
      ) { (acc, wrapper) =>
        val metrics = wrapper.info

        // Combine peakMemoryMetrics by taking the max of each metric
        val combinedPeakMemoryMetrics = (acc.peakMemoryMetrics, metrics.peakMemoryMetrics) match {
          case (Some(accMetrics), Some(newMetrics)) =>
            val combinedMetricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
              metric -> Math.max(
                accMetrics.getMetricValue(metric),
                newMetrics.getMetricValue(metric)
              )
            }
            Some(new ExecutorMetrics(combinedMetricsMap.toMap))
          case (None, newMetrics) => newMetrics
          case (accMetrics, None) => accMetrics
        }

        new ExecutorStageSummary(
          taskTime = acc.taskTime + metrics.taskTime,
          failedTasks = acc.failedTasks + metrics.failedTasks,
          succeededTasks = acc.succeededTasks + metrics.succeededTasks,
          killedTasks = acc.killedTasks + metrics.killedTasks,
          inputBytes = acc.inputBytes + metrics.inputBytes,
          inputRecords = acc.inputRecords + metrics.inputRecords,
          outputBytes = acc.outputBytes + metrics.outputBytes,
          outputRecords = acc.outputRecords + metrics.outputRecords,
          shuffleRead = acc.shuffleRead + metrics.shuffleRead,
          shuffleReadRecords = acc.shuffleReadRecords + metrics.shuffleReadRecords,
          shuffleWrite = acc.shuffleWrite + metrics.shuffleWrite,
          shuffleWriteRecords = acc.shuffleWriteRecords + metrics.shuffleWriteRecords,
          memoryBytesSpilled = acc.memoryBytesSpilled + metrics.memoryBytesSpilled,
          diskBytesSpilled = acc.diskBytesSpilled + metrics.diskBytesSpilled,
          isBlacklistedForStage = acc.isBlacklistedForStage || metrics.isBlacklistedForStage,
          peakMemoryMetrics = combinedPeakMemoryMetrics,
          isExcludedForStage = acc.isExcludedForStage || metrics.isExcludedForStage
        )
      }
    }

    // Rebuild executor summaries with aggregated metrics
    executorsInJob.map { executorWrapper =>
      val executorId = executorWrapper.info.id
      val aggregatedMetrics = sumMetricsForExecutor(executorId)

      // Adjust addTime and removeTime based on startTime and endTime
      val adjustedAddTime = if (executorWrapper.info.addTime.before(startTime)) startTime else executorWrapper.info.addTime
      val adjustedRemoveTime = executorWrapper.info.removeTime match {
        case Some(removeTime) if removeTime.after(endTime) => Some(endTime)
        case other => other
      }

      // Create a new ExecutorSummary with updated metrics and adjusted times
      val updatedExecutorSummary = new ExecutorSummary(
        id = executorWrapper.info.id,
        hostPort = executorWrapper.info.hostPort,
        isActive = executorWrapper.info.isActive,
        rddBlocks = executorWrapper.info.rddBlocks,
        memoryUsed = executorWrapper.info.memoryUsed,
        diskUsed = executorWrapper.info.diskUsed,
        totalCores = executorWrapper.info.totalCores,
        maxTasks = executorWrapper.info.maxTasks,
        activeTasks = executorWrapper.info.activeTasks,
        failedTasks = aggregatedMetrics.failedTasks,
        completedTasks = aggregatedMetrics.succeededTasks,
        totalTasks = aggregatedMetrics.succeededTasks + aggregatedMetrics.failedTasks,
        totalDuration = aggregatedMetrics.taskTime,
        totalGCTime = executorWrapper.info.totalGCTime, // Keep existing
        totalInputBytes = aggregatedMetrics.inputBytes,
        totalShuffleRead = aggregatedMetrics.shuffleRead,
        totalShuffleWrite = aggregatedMetrics.shuffleWrite,
        isBlacklisted = executorWrapper.info.isBlacklisted,
        maxMemory = executorWrapper.info.maxMemory,
        addTime = adjustedAddTime,
        removeTime = adjustedRemoveTime,
        removeReason = executorWrapper.info.removeReason,
        executorLogs = executorWrapper.info.executorLogs,
        memoryMetrics = executorWrapper.info.memoryMetrics,
        blacklistedInStages = executorWrapper.info.blacklistedInStages,
        peakMemoryMetrics = aggregatedMetrics.peakMemoryMetrics,
        attributes = executorWrapper.info.attributes,
        resources = executorWrapper.info.resources,
        resourceProfileId = executorWrapper.info.resourceProfileId,
        isExcluded = executorWrapper.info.isExcluded,
        excludedInStages = executorWrapper.info.excludedInStages
      )

      // Wrap the updated ExecutorSummary in ExecutorSummaryWrapper
      new ExecutorSummaryWrapper(updatedExecutorSummary)
    }
  }

  def readAllByKeys[T: ClassTag](keys: Seq[Object]): Seq[T] = {
    keys.flatMap(key =>
        Some(kvStore.read(classTag[T].runtimeClass, key))
    ).asInstanceOf[Seq[T]]
  }

  def readAllIfExists[T: ClassTag](keys: Seq[Object]): Seq[T] = {
    keys.flatMap(key =>
      try {
        Some(kvStore.read(classTag[T].runtimeClass, key))
      } catch {
        case _: NoSuchElementException => None
      }
    ).asInstanceOf[Seq[T]]
  }

  // Usage

  def writeAll[T](kvstore: KVStore, data: Seq[T]): Unit = {
    data.foreach(kvstore.write)
  }

  private def readAll[T: ClassTag]: Seq[T] = {
    val view = kvStore.view(classTag[T].runtimeClass)
    val it = view.closeableIterator()
    try {
      it.toSeq.asInstanceOf[Seq[T]]
    } finally {
      it.close()
    }
  }
}
