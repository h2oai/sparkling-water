package ai.h2o.sparkling.backend.api

import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.servlet.Servlet
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder, ServletMapping}

private[api] trait ServletRegister {
  protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet

  protected def getRequestPaths(): Array[String]

  def register(context: ServletContextHandler, conf: H2OConf, hc: H2OContext): Unit = {
    val holder = new ServletHolder(getServlet(conf, hc))
    context.getServletHandler.addServlet(holder)
    val m = new ServletMapping()
    m.setPathSpecs(getRequestPaths())
    m.setServletName(holder.getName)
    context.getServletHandler.addServletMapping(m)
  }
}
