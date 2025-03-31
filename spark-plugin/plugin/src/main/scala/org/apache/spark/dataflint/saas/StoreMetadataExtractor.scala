package org.apache.spark.dataflint.saas

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, ExecutorSummary}
import org.apache.spark.util.Utils
import org.apache.spark.{JobExecutionStatus, SparkConf}

import java.util.Date

class StoreMetadataExtractor(store: AppStatusStore, sqlStore: SQLAppStatusStore, conf: SparkConf) {
  private val version: String = "1"
  private val dataflintStore = new DataflintStore(store.store)

  def extract(runId: String, accessKey: String, eventEndTime: Long): SparkMetadataStore = {
    SparkMetadataStore(
      version = version,
      runId = runId,
      accessKey = accessKey,
      applicationInfo = getUpdatedAppInfo(eventEndTime),
      metrics = calculateMetrics(eventEndTime),
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

  def calculateMetrics(eventEndTime: Long): SparkMetadataMetrics = {
    def executorDurationMs(executor: ExecutorSummary): Long = {
      executor.removeTime.map(_.getTime).getOrElse(eventEndTime) - executor.addTime.getTime
    }

    val allExecutors = store.executorList(false)
    val onlyExecutors = store.executorList(false).filter(_.id != "driver")
    // in standalone mode, when allExecutors.length, the driver is part of the compute. If not, the driver is not taking tasks and not part of the compute.
    val computeExecutors = if (allExecutors.length == 1) allExecutors else onlyExecutors
    val driver = store.executorList(false).find(_.id == "driver").head
    val sqlQueries = sqlStore.executionsList()
    val stages = store.stageList(null)

    val totalInputBytes = allExecutors.map(_.totalInputBytes).sum
    val peakJvmExecutorsMemory = onlyExecutors
      .filter(_.peakMemoryMetrics.isDefined)
      .map(_.peakMemoryMetrics.get)
      .map(mem => mem.getMetricValue("JVMHeapMemory"))
    val peakContainerMemory = onlyExecutors
      .filter(_.peakMemoryMetrics.isDefined)
      .map(_.peakMemoryMetrics.get)
      .map(mem => mem.getMetricValue("JVMOffHeapMemory") + mem.getMetricValue("JVMOffHeapMemory"))

    val driverJvmPeakMemoryBytes = driver.peakMemoryMetrics.map(_.getMetricValue("JVMHeapMemory")).getOrElse(-1L)

    val executorJvmPeakMemoryBytes = if(peakJvmExecutorsMemory.isEmpty) 0 else peakJvmExecutorsMemory.max
    val containerPeakMemoryBytes = if(peakContainerMemory.isEmpty) 0 else peakContainerMemory.max

    val executorMemoryConf = conf.get(EXECUTOR_MEMORY)
    val memoryOverheadConf = conf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(386.toLong).toDouble
    val driverMemoryBytesConf = dataflintStore.environmentInfo().map(_.driverXmxBytes).getOrElse(conf.get(DRIVER_MEMORY) * 1024 * 1024)

    val memoryOverheadFactorConf = (executorMemoryConf * conf.getOption("spark.kubernetes.memoryOverheadFactor").map(_.toDouble).getOrElse(conf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)))
    val memoryOverhead = Math.max(memoryOverheadConf, memoryOverheadFactorConf)
    val executorJvmMemoryGb = executorMemoryConf.toDouble / 1024
    val containerMemoryGb = (executorMemoryConf + memoryOverhead).toDouble / 1024
    val driverMemoryGb = conf.get(DRIVER_MEMORY).toDouble / 1024

    val executorsDurationMs = onlyExecutors.map(executorDurationMs).sum
    val driverDurationMs = executorDurationMs(driver)
    val coreHourUsage = allExecutors.map(exec => (executorDurationMs(exec).toDouble / 1000 / 60 / 60).toDouble * exec.totalCores).sum
    val memoryGbHour = onlyExecutors.map(exec => (executorDurationMs(exec).toDouble / 1000 / 60 / 60).toDouble * containerMemoryGb).sum + ((executorDurationMs(driver).toDouble / 1000 / 60 / 60).toDouble * driverMemoryGb).toDouble
    val totalDCU = (memoryGbHour * 0.005) + (coreHourUsage * 0.05)
    val totalSpillBytes = stages.map(stage => stage.diskBytesSpilled).sum
    val totalOutputBytes = stages.map(stage => stage.outputBytes).sum

    val totalTasks = stages.map(stage => stage.numTasks).sum
    val failedTasks = stages.map(stage => stage.numFailedTasks).sum
    val taskErrorRate = calculatePercentage(failedTasks.toDouble, totalTasks.toDouble)

    val totalTasksSlotsMs = computeExecutors.map(exec => executorDurationMs(exec).toDouble * exec.maxTasks).sum
    val totalTaskTime = computeExecutors.map(_.totalDuration).sum
    val idleCoresRatio = 100 - calculatePercentage(totalTaskTime, totalTasksSlotsMs)

    val totalShuffleWriteBytes = onlyExecutors.map(exec => exec.totalShuffleWrite).sum
    val totalShuffleReadBytes = onlyExecutors.map(exec => exec.totalShuffleRead).sum

    val isAnySqlQueryFailed = sqlQueries.exists(query => query.jobs.exists { case (_, status) => status == JobExecutionStatus.FAILED })

    val containerMemoryUsage = calculatePercentage(containerPeakMemoryBytes, containerMemoryGb * 1024 * 1024 * 1024)
    val executorJvmMemoryUsage = calculatePercentage(executorJvmPeakMemoryBytes, executorJvmMemoryGb * 1024 * 1024 * 1024)
    val driverJvmMemoryUsage = calculatePercentage(driverJvmPeakMemoryBytes, driverMemoryBytesConf.toDouble)

    val rddStorageInfos = dataflintStore.rddStorageInfo()
    val totalCachedMemoryBytes = rddStorageInfos.map(_.memoryUsed).sum
    val totalCachedDiskBytes = rddStorageInfos.map(_.diskUsed).sum
    val maxExecutorCachedMemoryUsagePercentage = rddStorageInfos.map(rddStorageInfo => rddStorageInfo.maxMemoryExecutorInfo.map(_.memoryUsagePercentage).getOrElse(0.0)).max

    SparkMetadataMetrics(
      containerMemoryGb = containerMemoryGb,
      executorJvmMemoryGb = executorJvmMemoryGb,
      containerPeakMemoryBytes = containerPeakMemoryBytes,
      executorPeakMemoryBytes = executorJvmPeakMemoryBytes,
      containerMemoryUsage = containerMemoryUsage,
      executorJvmMemoryUsage = executorJvmMemoryUsage,
      totalInputBytes = totalInputBytes,
      totalOutputBytes = totalOutputBytes,
      totalSpillBytes = totalSpillBytes,
      totalShuffleWriteBytes = totalShuffleWriteBytes,
      totalShuffleReadBytes = totalShuffleReadBytes,
      coreHourUsage = coreHourUsage,
      memoryGbHour = memoryGbHour,
      totalDCU = totalDCU,
      isAnySqlQueryFailed = isAnySqlQueryFailed,
      taskErrorRate = taskErrorRate,
      idleCoresRatio = idleCoresRatio,
      CoresWastedRatio = idleCoresRatio, // for backward compatibility
      executorsDurationMs = executorsDurationMs,
      driverDurationMs = driverDurationMs,
      driverJvmPeakMemoryBytes = driverJvmPeakMemoryBytes,
      driverJvmMemoryUsage = driverJvmMemoryUsage,
      totalCachedMemoryBytes =totalCachedMemoryBytes,
      totalCachedDiskBytes = totalCachedDiskBytes,
      maxExecutorCachedMemoryUsagePercentage = maxExecutorCachedMemoryUsagePercentage
    )
  }
}
