package org.apache.spark

import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import org.apache.spark.ui.{SparkUI, UIUtils, WebUITab}

class DataflintTab(parent: SparkUI) extends WebUITab(parent,"dataflint") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
          <div>
          </div>
    UIUtils.basicSparkPage(request, content, "Devtool", true)
  }
}
