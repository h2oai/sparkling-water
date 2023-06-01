package ai.h2o.sparkling.backend.api.scalainterpreter

import ai.h2o.sparkling.backend.api.ParameterBase
import ai.h2o.sparkling.repl.H2OInterpreter
import javax.servlet.http.HttpServletRequest
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap

/** Schema representing [DELETE] /3/scalaint endpoint */
case class ScalaSessionId(session_id: Int, async: Boolean)

object ScalaSessionId extends ParameterBase {

  def apply(sessionId: Int): ScalaSessionId = ScalaSessionId(sessionId, async = false)

  private[scalainterpreter] case class ScalaSessionIdParameters(sessionId: Int) {
    def validate(mapIntr: TrieMap[Int, H2OInterpreter]): Unit = {
      if (!mapIntr.contains(sessionId)) {
        throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
      }
    }
  }

  object ScalaSessionIdParameters {
    private[scalainterpreter] def parse(request: HttpServletRequest): ScalaSessionIdParameters = {
      val sessionId = request.getPathInfo.drop(1).split("/").head.toInt
      ScalaSessionIdParameters(sessionId)
    }
  }
}
