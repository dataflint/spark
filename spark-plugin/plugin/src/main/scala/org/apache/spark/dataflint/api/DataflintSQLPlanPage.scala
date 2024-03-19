package org.apache.spark.dataflint.api

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.JsonAST.JValue
import org.json4s.{JsonAST, _}

import java.lang.reflect.Method
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

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

      val executionList = sqlStore.executionsList(offset.toInt, length.toInt)

      val isDatabricks = ui.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined
      val latestVersionReader = if(isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("latestVersion")) else None
      val planGraphReader = if(isDatabricks) Some(sqlStore.getClass.getMethods.filter(_.getName == "planGraph").head) else None
      val rddScopesToStagesReader = if(isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("rddScopesToStages")) else None

      var rddScopeIdReader: Option[Method] = None

      val sqlPlans = executionList.map { exec =>
        val graph = if (isDatabricks) {
          val planVersion = latestVersionReader.get.invoke(exec).asInstanceOf[Long]
          planGraphReader.get.invoke(sqlStore, exec.executionId.asInstanceOf[Object], planVersion.asInstanceOf[Object]).asInstanceOf[SparkPlanGraph]
        } else
            sqlStore.planGraph(exec.executionId)

        val rddScopesToStages = if(isDatabricks) Some(rddScopesToStagesReader.get.invoke(exec).asInstanceOf[Map[String, Set[Object]]]) else None

        SqlEnrichedData(exec.executionId, graph.allNodes.length, rddScopesToStages,
          graph.allNodes.map(node => {
            val rddScopeId = if(isDatabricks) {
              if(rddScopeIdReader.isEmpty) {
                rddScopeIdReader = Some(node.getClass.getMethod("rddScopeId"))
              }
              Some(rddScopeIdReader.get.invoke(node).asInstanceOf[String])
            }
            else None
            NodePlan(node.id, node.desc, rddScopeId)
          }).toSeq
        )
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
