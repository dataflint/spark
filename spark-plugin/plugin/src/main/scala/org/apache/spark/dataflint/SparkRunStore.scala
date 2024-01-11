package org.apache.spark.dataflint

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.status._
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.KVIndex

case class StageTaskSummary(
  stageId: Int,
  stageAttemptId: Int,
  summary: v1.TaskMetricDistributions) {
  @KVIndex
  @JsonIgnore
  def id: Array[Any] = Array(stageId, stageAttemptId)
}

case class SparkRunStore(
                          applicationInfos: Seq[ApplicationInfoWrapper],
                          applicationEnvironmentInfo: Seq[ApplicationEnvironmentInfoWrapper],
                          resourceProfiles: Seq[ResourceProfileWrapper],
                          jobDatas: Seq[JobDataWrapper],
                          stageDatas: Seq[StageDataWrapper],
                          executorSummaries: Seq[ExecutorSummaryWrapper],
                          taskDatas: Seq[TaskDataWrapper],
                          rddStorageInfos: Seq[RDDStorageInfoWrapper],
                          streamBlockDatas: Seq[StreamBlockData],
                          rddOperationGraphs: Seq[RDDOperationGraphWrapper],
                          poolDatas: Seq[PoolData],
                          appSummaries: Seq[AppSummary],
                          executorStageSummaries: Seq[ExecutorStageSummaryWrapper],
                          speculationStageSummaries: Seq[SpeculationStageSummaryWrapper],
                          sparkPlanGraphWrapper: Seq[SparkPlanGraphWrapper],
                          sqlExecutionUIData: Seq[SQLExecutionUIData],
                          stageTaskSummary: Seq[StageTaskSummary]
                        )
