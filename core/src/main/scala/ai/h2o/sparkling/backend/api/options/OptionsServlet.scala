package ai.h2o.sparkling.backend.api.options

import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all H2OFrame related queries
  */
private[api] class OptionsServlet(conf: H2OConf) extends ServletBase {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null =>
        val parameters = Option.OptionParameters.parse(req)
        parameters.validate(conf)
        getOptionValue(parameters.name)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  private def getOptionValue(name: String): Option = {
    Option(name, conf.get(name))
  }
}

object OptionsServlet extends ServletRegister {
  override protected def getRequestPaths(): Array[String] = Array("/3/option")

  override protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet = new OptionsServlet(conf: H2OConf)
}
