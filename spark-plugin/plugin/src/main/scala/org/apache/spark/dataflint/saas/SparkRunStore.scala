package org.apache.spark.dataflint.saas

import org.apache.spark.dataflint.listener.{DatabricksAdditionalExecutionWrapper, DataflintEnvironmentInfoEvent, DataflintEnvironmentInfoWrapper, DataflintRDDStorageInfoWrapper, IcebergCommitWrapper}
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.status._

case class SparkRunStore(
                          version: String,
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
                          stageTaskSummary: Seq[StageTaskSummary],
                          databricksAdditionalExecutionInfo: Seq[DatabricksAdditionalExecutionWrapper],
                          icebergCommit: Seq[IcebergCommitWrapper],
                          dataflintEnvironmentInfo: Seq[DataflintEnvironmentInfoWrapper],
                          dataflintRDDStorageInfo: Seq[DataflintRDDStorageInfoWrapper]
                        )
