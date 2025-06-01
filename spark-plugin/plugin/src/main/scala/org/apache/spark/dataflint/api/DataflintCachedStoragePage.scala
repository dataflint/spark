package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.{DataflintExecutorStorageInfo, DataflintRDDStorageInfo, DataflintStore}
import org.apache.spark.internal.Logging
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JObject}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataflintCachedStoragePage(ui: SparkUI, dataflintStore: DataflintStore)
  extends WebUIPage("cachedstorage") with Logging {
  override def renderJson(request: HttpServletRequest) = {
    try {
      val liveRddStorage = ui.store.rddList()
      val rddStorage = dataflintStore.rddStorageInfo()
      val graphs = ui.store.stageList(null)
        .filter(_.submissionTime.isDefined) // filter skipped or pending stages
        .map(stage => Tuple2(stage.stageId,
          ui.store.operationGraphForStage(stage.stageId).rootCluster.childClusters.flatMap(_.childNodes)
            .filter(_.cached)
            .map(rdd => {

              val liveCached = liveRddStorage.find(_.id == rdd.id).map(
                rdd => {
                  val maxUsageExecutor =  rdd.dataDistribution.map(executors => executors.maxBy(_.memoryUsed))
                  val maxExecutorUsage = maxUsageExecutor.map(executor =>
                    DataflintExecutorStorageInfo(
                      executor.memoryUsed,
                      executor.memoryRemaining,
                      if(executor.memoryUsed + executor.memoryRemaining != 0) executor.memoryUsed.toDouble / (executor.memoryUsed + executor.memoryRemaining) * 100 else 0
                  ))
                  DataflintRDDStorageInfo(rdd.id,
                                          rdd.memoryUsed,
                                          rdd.diskUsed,
                                          rdd.numPartitions,
                                          rdd.storageLevel,
                                          maxExecutorUsage
                )}
              )
              val cached = rddStorage.find(_.rddId == rdd.id)
              liveCached.getOrElse(cached)
            }))).toMap
      val jsonValue = Extraction.decompose(graphs)(org.json4s.DefaultFormats)
      jsonValue
    }
    catch {
      case e: Throwable => {
        logError("failed to serve dataflint Jobs RDD", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
