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

package org.apache.spark.status.api.v1.dataflint

import org.apache.spark.sql.execution.ui._
import org.apache.spark.status.api.v1.BaseAppResource

import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class DataflintSqlResource extends BaseAppResource {

  @GET
  @Path("{executionId:\\d+}")
  def sql(
           @PathParam("executionId") executionId: Long): Seq[NodeMetrics] = {
    withUI { ui =>
      val sqlListener = ui.sc.flatMap(_.listenerBus.listeners.toArray().find(_.isInstanceOf[SQLAppStatusListener]).asInstanceOf[Option[SQLAppStatusListener]])
      val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListener)
      val metrics = sqlStore.executionMetrics(executionId)
      val graph = sqlStore.planGraph(executionId)
      val nodesMetrics = graph.nodes.map(node => NodeMetrics(node.id, node.name, node.metrics.map(metric => {
          NodeMetric(metric.name, metrics.get(metric.accumulatorId))
        })))
        // filter nodes without metrics
        .filter(nodeMetrics => !nodeMetrics.metrics.forall(_.value.isEmpty))
      nodesMetrics
    }

  }

}