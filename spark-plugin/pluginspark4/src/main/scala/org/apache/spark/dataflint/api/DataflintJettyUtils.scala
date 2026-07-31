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
  // we use reflection to support the different Jetty package layouts Spark has shipped:
  //   - org.sparkproject.jetty.ee10.servlet  (Spark 4.2+, shaded Jetty 12 / EE10)
  //   - org.sparkproject.jetty.servlet       (Spark 3.x / 4.0 / 4.1, shaded Jetty 9-11)
  //   - org.eclipse.jetty[.ee10].servlet     (unshaded fallbacks)
  private def createStaticHandler(resourceBase: String, path: String): Any = {
    // Try to load the class from every Jetty package layout Spark has used, most-recent first.
    def getClassForName(className: String): Class[_] = {
      val candidates = Seq(
        s"org.sparkproject.jetty.ee10.servlet.$className",
        s"org.sparkproject.jetty.servlet.$className",
        s"org.eclipse.jetty.ee10.servlet.$className",
        s"org.eclipse.jetty.servlet.$className"
      )
      candidates.iterator
        .flatMap { name =>
          try Some(Class.forName(name))
          catch { case _: ClassNotFoundException => None }
        }
        .nextOption()
        .getOrElse(throw new ClassNotFoundException(
          s"Could not load Jetty class '$className' from any known package: ${candidates.mkString(", ")}"))
    }

    val servletContextHandlerClass = getClassForName("ServletContextHandler")
    val defaultServletClass = getClassForName("DefaultServlet")
    val servletHolderClass = getClassForName("ServletHolder")

    // Jetty 12 (EE10) renamed the DefaultServlet resource init-param from "resourceBase" to
    // "baseResource" and moved the gzip init-param under the ee10 package namespace.
    val isEe10 = servletContextHandlerClass.getName.contains(".ee10.")
    val gzipInitParam =
      if (isEe10) "org.eclipse.jetty.ee10.servlet.Default.gzip"
      else "org.eclipse.jetty.servlet.Default.gzip"
    val resourceBaseInitParam = if (isEe10) "baseResource" else "resourceBase"

    val contextHandler = servletContextHandlerClass.getDeclaredConstructor().newInstance()
    val setInitParameterMethod = contextHandler.getClass.getMethod("setInitParameter", classOf[String], classOf[String])
    setInitParameterMethod.invoke(contextHandler, gzipInitParam, "false")

    val staticHandler = defaultServletClass.getDeclaredConstructor().newInstance()
    val servletHolderConstructor = servletHolderClass.getConstructor(classOf[Servlet])
    val holder = servletHolderConstructor.newInstance(staticHandler.asInstanceOf[Object])

    Option(this.getClass.getClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        val setInitParameterMethodForHolder = holder.getClass.getMethod("setInitParameter", classOf[String], classOf[String])
        setInitParameterMethodForHolder.invoke(holder, resourceBaseInitParam, res.toString)
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
