package org.apache.spark.dataflint.saas

import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.status._

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.reflect.{ClassTag, classTag}

class StoreDataExtractor(store: AppStatusStore) {
  private val version: String = "1"
  private val kvStore = store.store.asInstanceOf[ElementTrackingStore]

  def extract(): SparkRunStore = {
    SparkRunStore(
      version = version,
      applicationInfos = readAll[ApplicationInfoWrapper],
      applicationEnvironmentInfo = readAll[ApplicationEnvironmentInfoWrapper],
      resourceProfiles = readAll[ResourceProfileWrapper],
      jobDatas = readAll[JobDataWrapper],
      stageDatas = readAll[StageDataWrapper],
      executorSummaries = readAll[ExecutorSummaryWrapper],
      taskDatas = readAll[TaskDataWrapper],
      rddStorageInfos = readAll[RDDStorageInfoWrapper],
      streamBlockDatas = readAll[StreamBlockData],
      rddOperationGraphs = readAll[RDDOperationGraphWrapper],
      poolDatas = readAll[PoolData],
      appSummaries = readAll[AppSummary],
      executorStageSummaries = readAll[ExecutorStageSummaryWrapper],
      speculationStageSummaries = readAll[SpeculationStageSummaryWrapper],
      sparkPlanGraphWrapper = readAll[SparkPlanGraphWrapper],
      sqlExecutionUIData = readAll[SQLExecutionUIData],
      stageTaskSummary = calculateTaskSummary()
    )
  }

  private def calculateTaskSummary(): Seq[StageTaskSummary]  = {
    val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0)
    store.stageList(null).map(stage => {
      store.taskSummary(stage.stageId, stage.attemptId,quantiles).map(
        StageTaskSummary(stage.stageId, stage.attemptId, _)
      )
    }).filter(_.isDefined).map(_.get)
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
