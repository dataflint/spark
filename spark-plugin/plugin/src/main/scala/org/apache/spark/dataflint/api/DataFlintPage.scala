package org.apache.spark.dataflint.api

import jakarta.servlet.http.HttpServletRequest
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JObject, JValue}

import scala.xml.Node

class DataFlintPage(ui: SparkUI, pageName: String, renderer: HttpServletRequest => JValue)
  extends WebUIPage("stagesrdd") with Logging {
  override def renderJson(request: HttpServletRequest): JValue = {
    try {
      val graphs = ui.store.stageList(null)
        .filter(_.submissionTime.isDefined) // filter skipped or pending stages
        .map(stage => Tuple2(stage.stageId,
          ui.store.operationGraphForStage(stage.stageId).rootCluster.childClusters
            .map(rdd => Tuple2(rdd.id, rdd.name)).toMap))
        .toMap
      val jsonValue: JValue = Extraction.decompose(graphs)(org.json4s.DefaultFormats)
      jsonValue
    }
    catch {
      case e: Throwable => {
        logError("failed to serve dataflint Jobs RDD", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
