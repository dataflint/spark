package org.apache.spark.dataflint

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.util.Utils

import java.util.Date

class StoreMetadataExtractor(store: AppStatusStore, conf: SparkConf) {
  def extract(runId: String, eventEndTime: Long): SparkMetadataStore = {

    SparkMetadataStore(
      runId = runId,
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

  def calculateMetrics(): SparkMetadataMetrics = {
    val allExecutors = store.executorList(false)
    val onlyExecutors = store.executorList(false).filter(_.id != "driver")
    val driver = store.executorList(false).find(_.id == "driver").head

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

    SparkMetadataMetrics(
      containerMemoryGb = containerMemoryGb,
      totalInputBytes = totalInputBytes,
      executorPeakMemoryBytes = executorPeakMemoryBytes,
      coreHourUsage = coreHourUsage,
      memoryGbHour = memoryGbHour,
      totalDCU = totalDCU
    )
  }
}
