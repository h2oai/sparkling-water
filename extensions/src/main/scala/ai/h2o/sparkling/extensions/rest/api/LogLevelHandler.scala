/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.rest.api.schema.LogLevelV3
import org.apache.log4j.{Level, LogManager}
import water.MRTask
import water.api.Handler
import water.util.Log

/**
 * This servlet class handles GET and PUT requests for the path /3/LogLevel
 */
final class LogLevelHandler extends Handler {

  def setLogLevel(version: Int, request: LogLevelV3): LogLevelV3 = {
    new MRTask() {
      override def setupLocal() {
        if (!Log.LVLS.contains(request.log_level)) {
          Log.warn(s"[$request.log_level] is not a supported log level.")
        } else {
          // Setup log4j
          LogManager.getLogger("water.default").setLevel(Level.toLevel(request.log_level))
          Log.setLogLevel(request.log_level)
          Log.info(s"Log level changed to [${request.log_level}].")
        }
      }
    }.doAllNodes()
  }

  def getLogLevel(version: Int, request: LogLevelV3): LogLevelV3 = {
    request.log_level = Log.LVLS(Log.getLogLevel)
    request
  }
}
