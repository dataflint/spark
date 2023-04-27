package org.apache.spark
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

class NewSparkUIPage() extends WebUIPage("newui") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
          <div>
            <div class="container-fluid">
                <p>new UI. </p>
            </div>
          </div>
    UIUtils.basicSparkPage(request, content, "New UI", true)
  }
}
