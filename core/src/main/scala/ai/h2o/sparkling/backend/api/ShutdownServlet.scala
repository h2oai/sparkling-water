package ai.h2o.sparkling.backend.api

import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for the /3/shutdown POST request
  */
private[api] class ShutdownServlet extends ServletBase {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null =>
        H2OContext.get().foreach(_.stop(stopSparkContext = true))
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }
}

object ShutdownServlet extends ServletRegister {

  override protected def getRequestPaths(): Array[String] = Array("/3/shutdown")

  override protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet = new ShutdownServlet
}
