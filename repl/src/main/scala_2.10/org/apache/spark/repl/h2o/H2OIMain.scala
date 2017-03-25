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

package org.apache.spark.repl.h2o


import java.io.File

import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.repl.{Main, SparkIMain}

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

/**
  * SparkIMain allowing multiple interpreters to coexist in parallel. Each line in repl is wrapped in a package which
  * name contains current session id
  */
private[repl] class H2OIMain private(initialSettings: Settings,
                                     interpreterWriter: IntpResponseWriter,
                                     sessionId: Int,
                                     propagateExceptions: Boolean = false) extends {
  override private[repl] val outputDir: File = H2OIMain.classOutputDirectory
} with SparkIMain(initialSettings, interpreterWriter, propagateExceptions) with H2OIMainHelper {

  setupClassNames(naming, sessionId)
}


object H2OIMain extends H2OIMainHelper {

  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]

  def createInterpreter(sc: SparkContext, settings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int): H2OIMain = synchronized {
    initializeClassLoader(sc)
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId, false))
    existingInterpreters(sessionId)
  }

  private lazy val _classOutputDirectory = {
    if (Main.interp != null) {
      // Application was started using shell, we can reuse this directory
      Main.interp.intp.getClassOutputDirectory
    } else {
      newREPLDirectory()
    }
  }
  def classOutputDirectory = _classOutputDirectory
}