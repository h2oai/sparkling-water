package ai.h2o.sparkling.backend.api.scalainterpreter

import ai.h2o.sparkling.backend.api.ParameterBase
import ai.h2o.sparkling.repl.H2OInterpreter
import javax.servlet.http.HttpServletRequest
import water.api.schemas3.JobV3
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap

/** Schema representing [POST] /3/scalaint/[session_id] endpoint. */
case class ScalaCode(
    session_id: Int,
    code: String,
    result_key: String,
    status: String,
    response: String,
    output: String,
    job: JobV3)

object ScalaCode extends ParameterBase {

  private[scalainterpreter] case class ScalaCodeParameters(sessionId: Int, code: String) {
    def validate(mapIntr: TrieMap[Int, H2OInterpreter]): Unit = {
      if (sessionId == -1 || !mapIntr.isDefinedAt(sessionId)) {
        throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
      }
    }
  }

  object ScalaCodeParameters {
    private[scalainterpreter] def parse(request: HttpServletRequest): ScalaCodeParameters = {
      val sessionId = request.getPathInfo.drop(1).split("/").head.toInt
      val code = getParameterAsString(request, "code")
      ScalaCodeParameters(sessionId, code)
    }
  }
}
