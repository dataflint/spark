package org.apache.spark.dataflint.api

case class ResolvedStageGroup(
  sparkStageId: Int,
  status: String,
  numTasks: Int,
  numCompleteTasks: Int,
  executorRunTime: Long,
  nodeIds: Seq[Long],
  exchangeWriteIds: Seq[Long],
  exchangeReadIds: Seq[Long]
)