package org.apache.spark.dataflint.api

import org.apache.spark.ui.SparkUI

import jakarta.servlet.Servlet
import scala.language.implicitConversions

object DataflintJettyUtils {
  // there is a conflict in the attachHandler jetty package name so we need to do this call with reflection
  def addStaticHandler(ui: SparkUI, resourceBase: String, path: String = "/static"): Unit = {
    val handler = createStaticHandler(resourceBase, path)
    val method = ui.getClass.getMethods.filter(s => s.getName == "attachHandler" && s.getParameterCount == 1).head
    method.invoke(ui, handler.asInstanceOf[Object])
  }

  // copy of createStaticHandler in core/src/main/scala/org/apache/spark/ui/JettyUtils.scala
  // only difference is we are loading the resources from this class loader which might be different from the spark one
  // with use reflection to support both org.sparkproject.jetty.servlet and org.eclipse.jetty
  // in spark source code
  private def createStaticHandler(resourceBase: String, path: String): Any = {
    // Try to load classes from both packages
    def getClassForName(className: String): Class[_] = {
      // Spark 4.2 upgraded to Jetty 12, which relocated the servlet classes into
      // the EE10 package (org.sparkproject.jetty.ee10.servlet.*). Spark 4.0/4.1
      // and 3.x use the older org.sparkproject.jetty.servlet.*, and an unshaded
      // Spark build uses org.eclipse.jetty.*. Probe all of them, newest first.
      val candidates = Seq(
        s"org.sparkproject.jetty.ee10.servlet.$className",
        s"org.sparkproject.jetty.servlet.$className",
        s"org.eclipse.jetty.ee10.servlet.$className",
        s"org.eclipse.jetty.servlet.$className"
      )
      candidates.iterator
        .map(n => try Some(Class.forName(n)) catch { case _: ClassNotFoundException => None })
        .collectFirst { case Some(c) => c }
        .getOrElse(throw new ClassNotFoundException(
          s"Could not locate Jetty class $className in any known package: ${candidates.mkString(", ")}"))
    }

    val servletContextHandlerClass = getClassForName("ServletContextHandler")
    val defaultServletClass = getClassForName("DefaultServlet")
    val servletHolderClass = getClassForName("ServletHolder")

    val contextHandler = servletContextHandlerClass.getDeclaredConstructor().newInstance()
    val setInitParameterMethod = contextHandler.getClass.getMethod("setInitParameter", classOf[String], classOf[String])
    setInitParameterMethod.invoke(contextHandler, "org.eclipse.jetty.servlet.Default.gzip", "false")

    val staticHandler = defaultServletClass.getDeclaredConstructor().newInstance()
    val servletHolderConstructor = servletHolderClass.getConstructor(classOf[Servlet])
    val holder = servletHolderConstructor.newInstance(staticHandler.asInstanceOf[Object])

    Option(this.getClass.getClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        val setInitParameterMethodForHolder = holder.getClass.getMethod("setInitParameter", classOf[String], classOf[String])
        setInitParameterMethodForHolder.invoke(holder, "resourceBase", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }

    val setContextPathMethod = contextHandler.getClass.getMethod("setContextPath", classOf[String])
    setContextPathMethod.invoke(contextHandler, path)

    val addServletMethod = contextHandler.getClass.getMethod("addServlet", servletHolderClass, classOf[String])
    addServletMethod.invoke(contextHandler, holder.asInstanceOf[Object], "/")

    contextHandler
  }

}
