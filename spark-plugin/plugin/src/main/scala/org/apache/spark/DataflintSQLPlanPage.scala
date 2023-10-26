package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.JsonAST

import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore}
import org.json4s.JsonAST.JValue
import org.json4s._

class DataflintSQLPlanPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener])
  extends WebUIPage("sqlplan") with Logging {
  private var sqlListenerCache: Option[SQLAppStatusListener] = None

  override def renderJson(request: HttpServletRequest): JsonAST.JValue = {
    try {
      if (sqlListenerCache.isEmpty) {
        sqlListenerCache = sqlListener()
      }
      val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)

      val offset = request.getParameter("offset")
      val length = request.getParameter("length")
      if (offset == null || length == null) {
        return JArray(List())
      }

      val sqlPlans = sqlStore.executionsList(offset.toInt, length.toInt).map { exec =>
        val graph = sqlStore.planGraph(exec.executionId)
        SqlEnrichedData(exec.executionId, graph.allNodes.length, graph.allNodes.map(node => NodePlan(node.id, node.desc)))
      }
      val jsonValue: JValue = Extraction.decompose(sqlPlans)(org.json4s.DefaultFormats)
      jsonValue
    }
    catch {
      case e: Throwable => {
        logError("failed to serve dataflint SQL metrics", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
