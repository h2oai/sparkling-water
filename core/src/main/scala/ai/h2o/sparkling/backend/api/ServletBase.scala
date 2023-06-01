package ai.h2o.sparkling.backend.api

import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.Gson
import javax.servlet.http.{HttpServlet, HttpServletResponse}

private[api] trait ServletBase extends HttpServlet {

  protected def sendResult(obj: Any, resp: HttpServletResponse): Unit = {
    val json = new Gson().toJson(obj)
    withResource(resp.getWriter) { writer =>
      resp.setContentType("application/json")
      resp.setCharacterEncoding("UTF-8")
      writer.print(json)
    }
    resp.setStatus(HttpServletResponse.SC_OK)
  }
}
