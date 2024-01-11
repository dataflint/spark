package org.apache.spark.dataflint

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.JsonAST.JValue
import org.json4s.{JsonAST, _}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataflintApplicationInfoPage(ui: SparkUI)
  extends WebUIPage("applicationinfo") with Logging {
  override def renderJson(request: HttpServletRequest): JsonAST.JValue = {
    try {
      val runIdConfigFromStore = ui.store.environmentInfo().sparkProperties.find(_._1 == "spark.dataflint.runId").map(_._2)
      val runIdPotentiallyFromConfig = if(runIdConfigFromStore.isEmpty) ui.conf.getOption("spark.dataflint.runId") else runIdConfigFromStore
      val applicationInfo = DataFlintApplicationInfo(runIdPotentiallyFromConfig, ui.store.applicationInfo())
      val jsonValue: JValue = Extraction.decompose(applicationInfo)(org.json4s.DefaultFormats)
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
