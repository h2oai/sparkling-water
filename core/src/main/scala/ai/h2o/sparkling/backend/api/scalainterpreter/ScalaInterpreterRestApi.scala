package ai.h2o.sparkling.backend.api.scalainterpreter

import java.net.URI

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.RestApiUtils

trait ScalaInterpreterRestApi extends RestApiUtils {

  protected def initSession(): ScalaSessionId = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[ScalaSessionId](endpoint, "/3/scalaint", hc.getConf)
  }

  protected def destroySession(sessionId: Int): Unit = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    delete[ScalaSessionId](endpoint, s"/3/scalaint/$sessionId", hc.getConf)
  }

  protected def getSessions(): ScalaSessions = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[ScalaSessions](endpoint, "/3/scalaint", hc.getConf)
  }

  protected def interpret(sessionId: Int, code: String): ScalaCode = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = Map("code" -> code)
    update[ScalaCode](endpoint, s"/3/scalaint/$sessionId", hc.getConf, params)
  }

  protected def getScalaCodeResult(resultKey: String): ScalaCodeResult = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[ScalaCodeResult](endpoint, s"/3/scalaint/result/$resultKey", hc.getConf)
  }
}
