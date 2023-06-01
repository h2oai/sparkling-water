package ai.h2o.sparkling.backend.api.scalainterpreter

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest;

/**
  * This object is returned by jobs executing the Scala code
  */
case class ScalaCodeResult(code: String, scalaStatus: String, scalaResponse: String, scalaOutput: String)

object ScalaCodeResult extends ParameterBase {

  private[scalainterpreter] case class ScalaCodeResultParameters(resultKey: String) {
    def validate(): Unit = {}
  }

  object ScalaCodeResultParameters {
    private[scalainterpreter] def parse(request: HttpServletRequest): ScalaCodeResultParameters = {
      val resultKey = request.getPathInfo.drop(1).split("/")(1)
      ScalaCodeResultParameters(resultKey)
    }
  }
}
