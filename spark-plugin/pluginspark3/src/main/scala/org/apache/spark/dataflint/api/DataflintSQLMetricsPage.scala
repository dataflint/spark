package org.apache.spark.dataflint.api

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JObject}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataflintSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener])
  extends WebUIPage("sqlmetrics") with Logging {
  private var sqlListenerCache: Option[SQLAppStatusListener] = None

  override def renderJson(request: HttpServletRequest) = {
    try {
      if (sqlListenerCache.isEmpty) {
        sqlListenerCache = sqlListener()
      }

      val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)
      val executionId = request.getParameter("executionId")
      if (executionId == null) {
        JObject()
      } else {
        val executionIdLong = executionId.toLong
        val metrics = sqlStore.executionMetrics(executionIdLong)
        val isDatabricks = ui.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined
        val graph = if (isDatabricks) {
          val exec = sqlStore.execution(executionIdLong).get
          val planVersion = exec.getClass.getMethod("latestVersion").invoke(exec).asInstanceOf[Long]
          sqlStore.getClass.getMethods.filter(_.getName == "planGraph").head.invoke(sqlStore, executionIdLong.asInstanceOf[Object], planVersion.asInstanceOf[Object]).asInstanceOf[SparkPlanGraph]
        } else
          sqlStore.planGraph(executionIdLong)
        val nodesMetrics = graph.allNodes.map(node => NodeMetrics(node.id, node.name, node.metrics.map(metric => {
            NodeMetric(metric.name, metrics.get(metric.accumulatorId))
          }).toSeq))
          // filter nodes without metrics
          .filter(nodeMetrics => !nodeMetrics.metrics.forall(_.value.isEmpty))
        val jValue = Extraction.decompose(nodesMetrics)(org.json4s.DefaultFormats)
        jValue
      }
    } catch {
      case e: Throwable => {
        logError("failed to serve dataflint SQL metrics", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
