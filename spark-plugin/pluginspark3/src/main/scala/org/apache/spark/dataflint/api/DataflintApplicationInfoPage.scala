package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JObject}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataflintApplicationInfoPage(ui: SparkUI, dataflintStore: DataflintStore)
  extends WebUIPage("applicationinfo") with Logging {
  override def renderJson(request: HttpServletRequest) = {
    try {
      val runIdConfigFromStore = ui.store.environmentInfo().sparkProperties.find(_._1 == "spark.dataflint.runId").map(_._2)
      val runIdPotentiallyFromConfig = if (runIdConfigFromStore.isEmpty) ui.conf.getOption("spark.dataflint.runId") else runIdConfigFromStore
      val applicationInfo = ui.store.applicationInfo()
      val environmentInfo = dataflintStore.environmentInfo()
      val dataFlintApplicationInfo = DataFlintApplicationInfo(runIdPotentiallyFromConfig, applicationInfo, environmentInfo)
      val jsonValue = Extraction.decompose(dataFlintApplicationInfo)(org.json4s.DefaultFormats)
      jsonValue
    }
    catch {
      case e: Throwable => {
        logError("failed to serve dataflint application info", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}