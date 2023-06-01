package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.rest.api.schema.LogLevelV3
import org.apache.log4j.{Level, LogManager}
import water.MRTask
import water.api.Handler
import water.util.Log

final class LogLevelHandler extends Handler {

  def setLogLevel(version: Int, request: LogLevelV3): LogLevelV3 = {
    new MRTask() {
      override def setupLocal() {
        if (!Log.LVLS.contains(request.log_level)) {
          Log.warn(s"Log level remains unchanged as [$request.log_level] is not a supported log level.")
        } else {
          LogManager.getLogger("water.default").setLevel(Level.toLevel(request.log_level))
          Log.setLogLevel(request.log_level)
          Log.info(s"Log level changed to [${request.log_level}].")
        }
      }
    }.doAllNodes()
    request
  }

  def getLogLevel(version: Int, request: LogLevelV3): LogLevelV3 = {
    request.log_level = Log.LVLS(Log.getLogLevel)
    request
  }
}
