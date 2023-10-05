package org.apache.spark

import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.JsonAST

import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore}
import org.json4s.JsonAST.JValue
import org.json4s._

class DataflintSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener]) extends WebUIPage("sqlmetrics") {
  private var sqlListenerCache: Option[SQLAppStatusListener] = None

  override def renderJson(request: HttpServletRequest): JsonAST.JValue = {
    if(sqlListenerCache.isEmpty){
      sqlListenerCache = sqlListener()
    }

    val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)
    val executionId = request.getParameter("executionId")
    if(executionId == null) {
      return JObject()
    }
    val executionIdLong = executionId.toLong
    val metrics = sqlStore.executionMetrics(executionIdLong)
    val graph = sqlStore.planGraph(executionIdLong)
    val nodesMetrics = graph.allNodes.map(node => NodeMetrics(node.id, node.name, node.metrics.map(metric => {
        NodeMetric(metric.name, metrics.get(metric.accumulatorId))
      })))
      // filter nodes without metrics
      .filter(nodeMetrics => !nodeMetrics.metrics.forall(_.value.isEmpty))
    val jValue: JValue = Extraction.decompose(nodesMetrics)(org.json4s.DefaultFormats)
    jValue
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
