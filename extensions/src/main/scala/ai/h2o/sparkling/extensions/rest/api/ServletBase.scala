package ai.h2o.sparkling.extensions.rest.api

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import water.server.ServletUtils

abstract class ServletBase extends HttpServlet {
  protected def getParameterAsString(request: HttpServletRequest, parameterName: String): String = {
    val result = request.getParameter(parameterName)
    if (result == null) {
      throw new IllegalArgumentException(s"Cannot find value for the parameter '$parameterName'")
    }
    result
  }

  protected def processRequest[R](request: HttpServletRequest, response: HttpServletResponse)(
      processor: => Unit): Unit = {
    val uri = ServletUtils.getDecodedUri(request)
    try {
      processor
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
    } catch {
      case e: Exception => ServletUtils.sendErrorResponse(response, e, uri)
    } finally {
      ServletUtils.logRequest(request.getMethod, request, response)
    }
  }
}
