package org.apache.spark.dataflint.api

import org.apache.spark.ui.{SparkUI, UIUtils, WebUITab}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataFlintTab(parent: SparkUI) extends WebUITab(parent,"dataflint") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
          <div>
          </div>
    UIUtils.basicSparkPage(request, content, "DataFlint", true)
  }
}
