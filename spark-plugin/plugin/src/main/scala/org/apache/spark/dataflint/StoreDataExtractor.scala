package org.apache.spark.dataflint

import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.status._
import org.apache.spark.util.kvstore.KVStore

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.reflect.{ClassTag, classTag}

class StoreDataExtractor(store: KVStore) {
  def extractAllData(): SparkRunStore = {
    SparkRunStore(
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
      sqlExecutionUIData = readAll[SQLExecutionUIData]
    )
  }

  private def readAll[T: ClassTag]: Seq[T] = {
    val view = store.view(classTag[T].runtimeClass)
    val it = view.closeableIterator()
    try {
      it.toSeq.asInstanceOf[Seq[T]]
    } finally {
      it.close()
    }
  }
}
