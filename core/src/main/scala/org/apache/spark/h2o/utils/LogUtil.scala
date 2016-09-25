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

package org.apache.spark.h2o.utils

import org.apache.log4j.{Level, LogManager}
import water.MRTask
import water.util.{Log, LogBridge}

/**
  * A simple helper to control H2O log subsystem
  */
object LogUtil extends org.apache.spark.internal.Logging {

  def setH2OClientLogLevel(level: String): Unit = {
    setLogLevel(
      level,
      () => logWarning(s"[$level] is not a supported log level."),
      () => logInfo(s"Log level changed to [$level]."))
  }

  def setH2ONodeLogLevel(level: String): Unit = {
    new MRTask() {
      override def setupLocal() {
        setLogLevel(
          level,
          () => Log.warn(s"[$level] is not a supported log level."),
          () => Log.info(s"Log level changed to [$level]."))
      }
    }.doAllNodes()
  }

  private def setLogLevel(level: String, logWarn: () => Unit, logChanged: () => Unit) = {
    val levelIdx = Log.valueOf(level)
    if (levelIdx < 0) {
      logWarn()
    } else {
      // Setup log4j
      LogManager.getLogger("water.default").setLevel(Level.toLevel(level))
      // Setup log level for H2O
      LogBridge.setH2OLogLevel(levelIdx)
      logChanged()
    }
  }
}

