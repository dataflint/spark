package org.apache.spark.dataflint.api

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import org.apache.spark.dataflint.listener.{DataflintExecutorStorageInfo, DataflintRDDStorageInfo, DataflintStore}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.ui.SparkUI
import org.json4s.{Extraction, JArray, JObject, JValue}
import org.json4s.jackson.JsonMethods.compact

/**
 * Build Jetty ServletContextHandlers that expose JSON endpoints without any
 * compile-time reference to javax.servlet or jakarta.servlet.
 *
 * Lets a single artifact run on both stock Spark 4 (jakarta) and Databricks
 * Runtime 17.3 (javax). The Servlet instance is a java.lang.reflect.Proxy
 * over the Servlet interface that exists on the classpath at runtime; the
 * Jetty ServletContextHandler/ServletHolder are loaded by reflection from
 * either org.sparkproject.jetty.servlet.* or org.eclipse.jetty.servlet.*.
 */
object DataflintReflectiveServletBuilder extends Logging {

  private lazy val servletApiPackage: String = {
    try { Class.forName("jakarta.servlet.Servlet"); "jakarta.servlet" }
    catch {
      case _: ClassNotFoundException =>
        Class.forName("javax.servlet.Servlet"); "javax.servlet"
    }
  }

  private def loadJettyClass(simpleName: String): Class[_] = {
    try Class.forName(s"org.sparkproject.jetty.servlet.$simpleName")
    catch {
      case _: ClassNotFoundException =>
        Class.forName(s"org.eclipse.jetty.servlet.$simpleName")
    }
  }

  /**
   * Build a ServletContextHandler that serves a single JSON payload at "/".
   *
   * @param contextPath  e.g. "/dataflint/applicationinfo/json"
   * @param jsonProducer takes a parameter accessor and returns a JValue
   * @return a Jetty ServletContextHandler instance (typed as Any to stay neutral)
   */
  def buildJsonHandler(contextPath: String,
                       jsonProducer: (String => String) => JValue): Any = {
    val servletInterface = Class.forName(s"$servletApiPackage.Servlet")
    val httpServletRequestClass = Class.forName(s"$servletApiPackage.http.HttpServletRequest")

    val invocationHandler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "service" =>
            try {
              val req = args(0)
              val resp = args(1)
              val getParameterMethod = httpServletRequestClass.getMethod("getParameter", classOf[String])
              val getParameter: String => String = (name: String) =>
                getParameterMethod.invoke(req, name).asInstanceOf[String]
              val body = compact(jsonProducer(getParameter))

              resp.getClass.getMethod("setContentType", classOf[String]).invoke(resp, "application/json")
              resp.getClass.getMethod("setStatus", classOf[Int]).invoke(resp, Int.box(200))
              val writer = resp.getClass.getMethod("getWriter").invoke(resp)
              writer.getClass.getMethod("print", classOf[String]).invoke(writer, body)
            } catch {
              case e: Throwable => logError(s"Failed to serve $contextPath", e)
            }
            null
          case _ => null
        }
      }
    }

    val servlet = Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(servletInterface),
      invocationHandler
    )

    val servletContextHandlerClass = loadJettyClass("ServletContextHandler")
    val servletHolderClass = loadJettyClass("ServletHolder")

    val ctx = servletContextHandlerClass.getDeclaredConstructor().newInstance().asInstanceOf[Object]
    ctx.getClass.getMethod("setContextPath", classOf[String]).invoke(ctx, contextPath)

    val holderCtor = servletHolderClass.getConstructors
      .find(c => c.getParameterCount == 1 && c.getParameterTypes()(0).isInstance(servlet))
      .getOrElse(sys.error(s"No 1-arg ServletHolder constructor accepting ${servlet.getClass.getName}"))
    val holder = holderCtor.newInstance(servlet.asInstanceOf[Object]).asInstanceOf[Object]

    ctx.getClass.getMethod("addServlet", servletHolderClass, classOf[String])
      .invoke(ctx, holder, "/")

    ctx
  }

  /**
   * Reflectively call SparkUI.attachHandler(ServletContextHandler).
   * Required because the parameter type is jakarta- or javax-bound at compile time
   * depending on which Spark we were built against.
   */
  def attachToUI(ui: SparkUI, handler: Any): Unit = {
    val attachHandlerMethod = ui.getClass.getMethods
      .find(m => m.getName == "attachHandler" && m.getParameterCount == 1)
      .getOrElse(sys.error("SparkUI has no attachHandler(handler) method"))
    attachHandlerMethod.invoke(ui, handler.asInstanceOf[Object])
  }

  private def attachEndpoint(ui: SparkUI,
                             basePath: String,
                             name: String,
                             producer: (String => String) => JValue): Unit = {
    val contextPath = basePath + s"/dataflint/$name/json"
    val handler = buildJsonHandler(contextPath, producer)
    attachToUI(ui, handler)
    logInfo(s"DataFlint reflective endpoint attached at $contextPath")
  }

  /**
   * Wire up every DataFlint JSON endpoint reflectively. Replaces the
   * WebUIPage-based flow used on Spark 3, so that the same artifact can
   * serve the JSON API on stock Spark 4 (jakarta.servlet) and on
   * Databricks Runtime 17.3 (javax.servlet).
   */
  def attachAllEndpoints(ui: SparkUI,
                         dataflintStore: DataflintStore,
                         sqlListener: () => Option[SQLAppStatusListener],
                         basePath: String): Unit = {
    attachApplicationInfoEndpoint(ui, dataflintStore, basePath)
    attachSqlPlanEndpoint(ui, dataflintStore, sqlListener, basePath)
    attachSqlMetricsEndpoint(ui, sqlListener, basePath)
    attachStagesRddEndpoint(ui, basePath)
    attachIcebergEndpoint(ui, dataflintStore, basePath)
    attachDeltaLakeScanEndpoint(ui, dataflintStore, basePath)
    attachCachedStorageEndpoint(ui, basePath, dataflintStore)
  }

  private def attachApplicationInfoEndpoint(ui: SparkUI,
                                            dataflintStore: DataflintStore,
                                            basePath: String): Unit = {
    val producer: (String => String) => JValue = _ => {
      try {
        val runIdConfigFromStore = ui.store.environmentInfo().sparkProperties
          .find(_._1 == "spark.dataflint.runId").map(_._2)
        val runIdPotentiallyFromConfig =
          if (runIdConfigFromStore.isEmpty) ui.conf.getOption("spark.dataflint.runId")
          else runIdConfigFromStore
        val applicationInfo = ui.store.applicationInfo()
        val environmentInfo = dataflintStore.environmentInfo()
        val dataFlintApplicationInfo =
          DataFlintApplicationInfo(runIdPotentiallyFromConfig, applicationInfo, environmentInfo)
        Extraction.decompose(dataFlintApplicationInfo)(org.json4s.DefaultFormats)
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint application info", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "applicationinfo", producer)
  }

  private def attachSqlPlanEndpoint(ui: SparkUI,
                                    dataflintStore: DataflintStore,
                                    sqlListener: () => Option[SQLAppStatusListener],
                                    basePath: String): Unit = {
    var sqlListenerCache: Option[SQLAppStatusListener] = None
    val producer: (String => String) => JValue = getParameter => {
      try {
        if (sqlListenerCache.isEmpty) {
          sqlListenerCache = sqlListener()
        }
        val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)

        val offset = getParameter("offset")
        val length = getParameter("length")
        if (offset == null || length == null) {
          JArray(List())
        } else {
          val executionList = sqlStore.executionsList(offset.toInt, length.toInt)

          val isDatabricks = ui.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined

          val latestVersionReader = if (isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("latestVersion")) else None
          val planGraphReader = if (isDatabricks) Some(sqlStore.getClass.getMethods.filter(_.getName == "planGraph").head) else None
          val rddScopesToStagesReader = if (isDatabricks && executionList.nonEmpty) Some(executionList.head.getClass.getMethod("rddScopesToStages")) else None

          val nodeIdToRddScopeIdList = dataflintStore.databricksAdditionalExecutionInfo(offset.toInt, length.toInt)

          val sqlPlans = executionList.flatMap { exec =>
            try {
              val graph = if (isDatabricks) {
                val planVersion = latestVersionReader.get.invoke(exec).asInstanceOf[Long]
                planGraphReader.get.invoke(sqlStore, exec.executionId.asInstanceOf[Object], planVersion.asInstanceOf[Object]).asInstanceOf[SparkPlanGraph]
              } else
                sqlStore.planGraph(exec.executionId)

              val rddScopesToStages = if (isDatabricks) Some(rddScopesToStagesReader.get.invoke(exec).asInstanceOf[Map[String, Set[Object]]]) else None

              val nodeIdToRddScopeId = nodeIdToRddScopeIdList.find(_.executionId == exec.executionId).map(_.nodeIdToRddScopeId)
              Some(SqlEnrichedData(exec.executionId, graph.allNodes.length, rddScopesToStages,
                graph.allNodes.map(node => {
                  val rddScopeId = nodeIdToRddScopeId.flatMap(_.get(node.id))
                  NodePlan(node.id, node.desc, rddScopeId)
                }).toSeq
              ))
            } catch {
              case _: Throwable => None
            }
          }
          Extraction.decompose(sqlPlans)(org.json4s.DefaultFormats)
        }
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint SQL metrics", e)
          JArray(List())
      }
    }
    attachEndpoint(ui, basePath, "sqlplan", producer)
  }

  private def attachSqlMetricsEndpoint(ui: SparkUI,
                                       sqlListener: () => Option[SQLAppStatusListener],
                                       basePath: String): Unit = {
    var sqlListenerCache: Option[SQLAppStatusListener] = None
    val producer: (String => String) => JValue = getParameter => {
      try {
        if (sqlListenerCache.isEmpty) {
          sqlListenerCache = sqlListener()
        }

        val sqlStore = new SQLAppStatusStore(ui.store.store, sqlListenerCache)
        val executionId = getParameter("executionId")
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
          Extraction.decompose(nodesMetrics)(org.json4s.DefaultFormats)
        }
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint SQL metrics", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "sqlmetrics", producer)
  }

  private def attachStagesRddEndpoint(ui: SparkUI, basePath: String): Unit = {
    val producer: (String => String) => JValue = _ => {
      try {
        val graphs = ui.store.stageList(null)
          .filter(_.submissionTime.isDefined) // filter skipped or pending stages
          .map(stage => Tuple2(stage.stageId,
            ui.store.operationGraphForStage(stage.stageId).rootCluster.childClusters
              .map(rdd => Tuple2(rdd.id, rdd.name)).toMap))
          .toMap
        Extraction.decompose(graphs)(org.json4s.DefaultFormats)
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint Jobs RDD", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "stagesrdd", producer)
  }

  private def attachIcebergEndpoint(ui: SparkUI,
                                    dataflintStore: DataflintStore,
                                    basePath: String): Unit = {
    val producer: (String => String) => JValue = getParameter => {
      try {
        val offset = getParameter("offset")
        val length = getParameter("length")
        if (offset == null || length == null) {
          JObject()
        } else {
          val commits = dataflintStore.icebergCommits(offset.toInt, length.toInt)
          val icebergInfo = IcebergInfo(commitsInfo = commits)
          Extraction.decompose(icebergInfo)(org.json4s.DefaultFormats)
        }
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint iceberg", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "iceberg", producer)
  }

  private def attachDeltaLakeScanEndpoint(ui: SparkUI,
                                          dataflintStore: DataflintStore,
                                          basePath: String): Unit = {
    val producer: (String => String) => JValue = getParameter => {
      try {
        val offset = getParameter("offset")
        val length = getParameter("length")
        if (offset == null || length == null) {
          JObject()
        } else {
          val scans = dataflintStore.deltaLakeScanInfo(offset.toInt, length.toInt)
          val deltaLakeInfo = DeltaLakeScanInfo(scans = scans)
          Extraction.decompose(deltaLakeInfo)(org.json4s.DefaultFormats)
        }
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint delta lake scan info", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "deltaLakeScans", producer)
  }

  private def attachCachedStorageEndpoint(ui: SparkUI,
                                          basePath: String,
                                          dataflintStore: DataflintStore): Unit = {
    val producer: (String => String) => JValue = _ => {
      try {
        val liveRddStorage = ui.store.rddList()
        val rddStorage = dataflintStore.rddStorageInfo()
        val graphs = ui.store.stageList(null)
          .filter(_.submissionTime.isDefined) // filter skipped or pending stages
          .map(stage => Tuple2(stage.stageId,
            ui.store.operationGraphForStage(stage.stageId).rootCluster.childClusters.flatMap(_.childNodes)
              .filter(_.cached)
              .map(rdd => {

                val liveCached = liveRddStorage.find(_.id == rdd.id).map(
                  rdd => {
                    val maxUsageExecutor =  rdd.dataDistribution.map(executors => executors.maxBy(_.memoryUsed))
                    val maxExecutorUsage = maxUsageExecutor.map(executor =>
                      DataflintExecutorStorageInfo(
                        executor.memoryUsed,
                        executor.memoryRemaining,
                        if(executor.memoryUsed + executor.memoryRemaining != 0) executor.memoryUsed.toDouble / (executor.memoryUsed + executor.memoryRemaining) * 100 else 0
                    ))
                    DataflintRDDStorageInfo(rdd.id,
                                            rdd.memoryUsed,
                                            rdd.diskUsed,
                                            rdd.numPartitions,
                                            rdd.storageLevel,
                                            maxExecutorUsage
                  )}
                )
                val cached = rddStorage.find(_.rddId == rdd.id)
                liveCached.getOrElse(cached)
              }))).toMap
        Extraction.decompose(graphs)(org.json4s.DefaultFormats)
      } catch {
        case e: Throwable =>
          logError("failed to serve dataflint Jobs RDD", e)
          JObject()
      }
    }
    attachEndpoint(ui, basePath, "cachedstorage", producer)
  }
}