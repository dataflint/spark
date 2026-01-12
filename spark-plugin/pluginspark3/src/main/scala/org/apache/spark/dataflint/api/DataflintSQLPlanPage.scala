package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JArray, JObject}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataflintSQLPlanPage(ui: SparkUI, dataflintStore: DataflintStore, sqlListener: () => Option[SQLAppStatusListener])
  extends WebUIPage("sqlplan") with Logging {
  private var sqlListenerCache: Option[SQLAppStatusListener] = None
  // Cache for rootExecutionId method - None means not yet checked, Some(None) means method doesn't exist, Some(Some(method)) means method exists
  private var rootExecutionIdMethodCache: Option[Option[java.lang.reflect.Method]] = None

  /**
   * Gets the rootExecutionId using reflection. Returns None if the method doesn't exist (Spark 3.3)
   * or if rootExecutionId equals executionId.
   */
  private def getRootExecutionId(exec: Any, executionId: Long): Option[Long] = {
    if (rootExecutionIdMethodCache.isEmpty) {
      rootExecutionIdMethodCache = Some(
        try {
          Some(exec.getClass.getMethod("rootExecutionId"))
        } catch {
          case _: NoSuchMethodException => None
        }
      )
    }

    rootExecutionIdMethodCache.get match {
      case Some(method) =>
        val rootId = method.invoke(exec).asInstanceOf[Long]
        if (rootId == executionId) None else Some(rootId)
      case None => None
    }
  }

  override def renderJson(request: HttpServletRequest) = {
    try {
      if (sqlListenerCache.isEmpty) {
        sqlListenerCache = sqlListener()
      }
      val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)

      val offset = request.getParameter("offset")
      val length = request.getParameter("length")
      if (offset == null || length == null) {
        JArray(List())
      } else {
        val executionList = sqlStore.executionsList(offset.toInt, length.toInt)

        val isDatabricks = ui.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined

        val latestVersionReader = if (isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("latestVersion")) else None
        val planGraphReader = if (isDatabricks) Some(sqlStore.getClass.getMethods.filter(_.getName == "planGraph").head) else None
        val rddScopesToStagesReader = if (isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("rddScopesToStages")) else None

        val nodeIdToRddScopeIdList = dataflintStore.databricksAdditionalExecutionInfo(offset.toInt, length.toInt)

        val sqlPlans = executionList.map { exec =>
          val graph = if (isDatabricks) {
            val planVersion = latestVersionReader.get.invoke(exec).asInstanceOf[Long]
            planGraphReader.get.invoke(sqlStore, exec.executionId.asInstanceOf[Object], planVersion.asInstanceOf[Object]).asInstanceOf[SparkPlanGraph]
          } else
            sqlStore.planGraph(exec.executionId)

          val rddScopesToStages = if (isDatabricks) Some(rddScopesToStagesReader.get.invoke(exec).asInstanceOf[Map[String, Set[Object]]]) else None

          val nodeIdToRddScopeId = nodeIdToRddScopeIdList.find(_.executionId == exec.executionId).map(_.nodeIdToRddScopeId)
          SqlEnrichedData(exec.executionId, getRootExecutionId(exec, exec.executionId), graph.allNodes.length, rddScopesToStages,
            graph.allNodes.map(node => {
              val rddScopeId = nodeIdToRddScopeId.flatMap(_.get(node.id))
              NodePlan(node.id, node.desc, rddScopeId)
            }).toSeq
          )
        }
        val jsonValue = Extraction.decompose(sqlPlans)(org.json4s.DefaultFormats)
        jsonValue
      }
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
