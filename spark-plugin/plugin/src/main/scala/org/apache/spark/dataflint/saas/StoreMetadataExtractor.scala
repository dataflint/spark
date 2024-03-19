package org.apache.spark.dataflint.saas

import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.util.Utils
import org.apache.spark.{JobExecutionStatus, SparkConf}

import java.util.Date

class StoreMetadataExtractor(store: AppStatusStore, sqlStore: SQLAppStatusStore, conf: SparkConf) {
  private val version: String = "1"

  def extract(runId: String, accessKey: String, eventEndTime: Long): SparkMetadataStore = {
    SparkMetadataStore(
      version = version,
      runId = runId,
      accessKey = accessKey,
      applicationInfo = getUpdatedAppInfo(eventEndTime),
      metrics = calculateMetrics(),
      conf = filterConf()
    )
  }

  // Copied from spark code in AppStatusListener to avoid a race condition
  def getUpdatedAppInfo(eventEndTime: Long): ApplicationInfo = {
    val appInfo = store.applicationInfo()
    val old = appInfo.attempts.head
    val attempt = ApplicationAttemptInfo(
      old.attemptId,
      old.startTime,
      new Date(eventEndTime),
      new Date(eventEndTime),
      eventEndTime - old.startTime.getTime(),
      old.sparkUser,
      true,
      old.appSparkVersion)

    ApplicationInfo(
      appInfo.id,
      appInfo.name,
      None,
      None,
      None,
      None,
      Seq(attempt))
  }

  def filterConf(): Map[String, String] = {
    Utils.redact(conf, store.environmentInfo().sparkProperties).sortBy(_._1).toMap
  }

  def calculatePercentage(base: Double, divider: Double): Double = {
    val percentage = if(divider != 0) (base / divider) * 100 else 0.0
    Math.max(Math.min(percentage, 100), 0)
  }

  def calculateMetrics(): SparkMetadataMetrics = {
    val allExecutors = store.executorList(false)
    val onlyExecutors = store.executorList(false).filter(_.id != "driver")
    val driver = store.executorList(false).find(_.id == "driver").head
    val sqlQueries = sqlStore.executionsList()
    val stages = store.stageList(null)

    val totalInputBytes = allExecutors.map(_.totalInputBytes).sum
    val peakExecutorsMemory = onlyExecutors
      .filter(_.peakMemoryMetrics.isDefined)
      .map(_.peakMemoryMetrics.get)
      .map(mem => mem.getMetricValue("JVMHeapMemory") + mem.getMetricValue("JVMOffHeapMemory"))
    val executorPeakMemoryBytes = if(peakExecutorsMemory.isEmpty) 0 else peakExecutorsMemory.max

    val executorMemoryConf = conf.get(EXECUTOR_MEMORY)
    val memoryOverheadConf = conf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(386.toLong).toDouble
    val memoryOverheadFactorConf = (executorMemoryConf * conf.getOption("spark.kubernetes.memoryOverheadFactor").map(_.toDouble).getOrElse(conf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)))
    val memoryOverhead = Math.max(memoryOverheadConf, memoryOverheadFactorConf)
    val containerMemoryGb = (executorMemoryConf + memoryOverhead).toDouble / 1024
    val driverMemoryGb = conf.get(DRIVER_MEMORY).toDouble / 1024

    val coreHourUsage = allExecutors.map(exec => (exec.totalDuration.toDouble / 1000 / 60 / 60) * exec.totalCores).sum
    val memoryGbHour = onlyExecutors.map(exec => (exec.totalDuration.toDouble / 1000 / 60 / 60).toDouble * containerMemoryGb).sum + ((driver.totalDuration.toDouble / 1000 / 60 / 60) * driverMemoryGb)
    val totalDCU = (memoryGbHour * 0.005) + (coreHourUsage * 0.05)
    val totalSpillBytes = stages.map(stage => stage.diskBytesSpilled).sum
    val totalOutputBytes = stages.map(stage => stage.outputBytes).sum

    val totalTasks = stages.map(stage => stage.numTasks).sum
    val failedTasks = stages.map(stage => stage.numFailedTasks).sum
    val taskErrorRate = calculatePercentage(failedTasks.toDouble, totalTasks.toDouble)

    val totalTasksSlotsMs = allExecutors.map(exec => (exec.totalDuration.toDouble) * exec.totalTasks).sum
    val totalTaskTime = stages.map(stage => stage.executorRunTime).sum
    val CoresWastedRatio = calculatePercentage(totalTaskTime, totalTasksSlotsMs)

    val totalShuffleWriteBytes = onlyExecutors.map(exec => exec.totalShuffleWrite).sum
    val totalShuffleReadBytes = onlyExecutors.map(exec => exec.totalShuffleRead).sum

    val isAnySqlQueryFailed = sqlQueries.exists(query => query.jobs.exists { case (_, status) => status == JobExecutionStatus.FAILED })

    SparkMetadataMetrics(
      containerMemoryGb = containerMemoryGb,
      totalInputBytes = totalInputBytes,
      totalOutputBytes = totalOutputBytes,
      totalSpillBytes = totalSpillBytes,
      totalShuffleWriteBytes = totalShuffleWriteBytes,
      totalShuffleReadBytes = totalShuffleReadBytes,
      executorPeakMemoryBytes = executorPeakMemoryBytes,
      coreHourUsage = coreHourUsage,
      memoryGbHour = memoryGbHour,
      totalDCU = totalDCU,
      isAnySqlQueryFailed = isAnySqlQueryFailed,
      taskErrorRate = taskErrorRate,
      CoresWastedRatio = CoresWastedRatio
    )
  }
}
