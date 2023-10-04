package org.apache.spark

import org.apache.spark.ui.SparkUI
import org.sparkproject.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import scala.language.implicitConversions

object DataflintJettyUtils {
  // there is a conflict in the attachHandler jetty package name so we need to do this call with reflection
  def addStaticHandler(ui: SparkUI, resourceBase: String, path: String = "/static"): Unit = {
    val handler = createStaticHandler(resourceBase, path)
    val method = ui.getClass.getMethods.filter(s => s.getName == "attachHandler" && s.getParameterCount == 1).head
    method.invoke(ui, handler)
  }

  // copy of createStaticHandler in core/src/main/scala/org/apache/spark/ui/JettyUtils.scala
  // in spark source code
  private def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    Option(this.getClass.getClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }
}
