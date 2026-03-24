/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.{IcebergCommitInfo, DataflintEnvironmentInfo, DataflintDeltaLakeScanInfo}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationInfo, StageData}

case class NodeMetric(name: String, value: Option[String])

case class NodeMetrics(id: Long, name: String, metrics: Seq[NodeMetric])

case class SqlEnrichedData(executionId: Long, rootExecutionId: Option[Long], numOfNodes:Int, rddScopesToStages: Option[Map[String, Set[Object]]], nodesPlan: Seq[NodePlan])

case class NodePlan(id: Long, planDescription: String, rddScopeId: Option[String])

case class DataFlintApplicationInfo(runId: Option[String], info: ApplicationInfo, environmentInfo: Option[DataflintEnvironmentInfo])

case class IcebergInfo(commitsInfo: Seq[IcebergCommitInfo])

case class DeltaLakeScanInfo(scans: Seq[DataflintDeltaLakeScanInfo])

/**
 * Compatibility helper for AppStatusStore methods whose signatures changed across Spark 3.x.
 */
object AppStatusStoreCompat {
  // In Spark 3.2+ stageList has a second `includeSkipped: Boolean = false` parameter.
  // The Scala compiler generates a `stageList$default$2()` call even for `stageList(null)`,
  // which causes NoSuchMethodError at runtime on Spark 3.1.x.
  // We use reflection so the call works on all Spark 3.x versions.
  def stageList(store: AppStatusStore): Seq[StageData] = {
    try {
      store.stageList(null)
    } catch {
      case _: NoSuchMethodError =>
        // Spark 3.1.x has a single-param stageList(statuses); the multi-param version was added in 3.2
        val m = store.getClass.getMethod("stageList", classOf[java.util.List[_]])
        m.invoke(store, null).asInstanceOf[Seq[StageData]]
    }
  }
}
