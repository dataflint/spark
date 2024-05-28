package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart, SparkPlanGraph, SparkPlanGraphNode}

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

object DataflintDatabricksLiveListener {
  def apply(listenerBus: LiveListenerBus): DataflintDatabricksLiveListener = {
    val rddScopeIdReader = classOf[SparkPlanGraphNode].getMethod("rddScopeId")
    new DataflintDatabricksLiveListener(listenerBus, rddScopeIdReader)
  }
}

class DataflintDatabricksLiveListener(listenerBus: LiveListenerBus, rddScopeIdReader: Method) extends SparkListener with Logging {
  private val executionToLatestVersion = new ConcurrentHashMap[Long, AtomicInteger]()

  private def publishDatabricksAdditionalSQLEvent(sparkPlanInfo: SparkPlanInfo, executionId: Long, version: Long): Unit = {
    val planGraph = SparkPlanGraph(sparkPlanInfo)
    val nodesToRddScopeId = planGraph.allNodes.map(node => {
      val rddScopeId = rddScopeIdReader.invoke(node).asInstanceOf[String]
      node.id -> rddScopeId
    }).toMap
    val executionInfo = DatabricksAdditionalExecutionInfo(executionId, version, nodesToRddScopeId)
    val event = DatabricksAdditionalExecutionEvent(executionInfo)
    listenerBus.post(event)
  }

  def onExecutionStart(e: SparkListenerSQLExecutionStart): Unit = {
    executionToLatestVersion.put(e.executionId, new AtomicInteger(0))
    publishDatabricksAdditionalSQLEvent(e.sparkPlanInfo, e.executionId, 0L)
  }

  def onAdaptiveExecutionUpdate(e: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val version = executionToLatestVersion.get(e.executionId).incrementAndGet()
    publishDatabricksAdditionalSQLEvent(e.sparkPlanInfo, e.executionId, version)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    try {
      event match {
        case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
        case e: SparkListenerSQLAdaptiveExecutionUpdate => onAdaptiveExecutionUpdate(e)
        case _ => {}
      }
    } catch {
      case e: Exception => logError("Error while processing events in DataflintDatabricksLiveListener", e)
    }
  }
}
