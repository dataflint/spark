package org.apache.spark.dataflint.api

import org.apache.spark.ui.{SparkUI, UIUtils, WebUITab}

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class DataFlintTab(parent: SparkUI) extends WebUITab(parent,"dataflint") {
  override val name: String = "DataFlint"
  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
          <div>
          </div>
    UIUtils.basicSparkPage(request, content, "DataFlint", true)
  }
}
