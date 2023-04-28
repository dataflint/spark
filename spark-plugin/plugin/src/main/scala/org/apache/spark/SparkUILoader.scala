package org.apache.spark

object SparkUILoader {
  def load(context: SparkContext): String = {
    context.ui.get.addStaticHandler("com/upgraydd/spark/static/ui", "/upgraydd")
    context.ui.get.webUrl
  }
}

//  val handler = createStaticHandler("/com/menis/spark/static/ui", "/custom-ui")
//  // context.ui.get.addStaticHandler("site", "example/site")
//  //context.ui.get.addStaticHandler("size", )
//  println("before")
//  val method = context.ui.get.getClass.getMethods().filter(s => s.getName == "attachHandler" && s.getParameterCount == 1).head
//  method.invoke(context.ui.get, handler)
//  println("after")
//  // context.ui.get.attachHandler(handler)
//
//  def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
//    val contextHandler = new ServletContextHandler
//    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
//    val staticHandler = new DefaultServlet
//    val holder = new ServletHolder(staticHandler)
//    val uri = this.getClass.getResource(resourceBase).toURI.toString
//    //val uri =  "file:/C:/Users/User/Documents/GitHub/spark-dev-tool/spark-plugin/plugin/target/scala-2.12/classes/com.menis.spark.static.ui"
//    holder.setInitParameter("resourceBase", uri)
//    contextHandler.setContextPath(path)
//    contextHandler.addServlet(holder, "/")
//    contextHandler
//  }
