package org.apache.spark.dataflint.saas

import com.fasterxml.jackson.annotation.JsonIgnore
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
