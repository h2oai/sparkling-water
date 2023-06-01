package ai.h2o.sparkling.backend.api

import javax.servlet.http.HttpServletRequest

trait ParameterBase {
  protected def getParameterAsString(request: HttpServletRequest, parameterName: String): String = {
    val result = request.getParameter(parameterName)
    if (result == null) {
      throw new IllegalArgumentException(s"Cannot find value for the parameter '$parameterName'")
    }
    result
  }
}
