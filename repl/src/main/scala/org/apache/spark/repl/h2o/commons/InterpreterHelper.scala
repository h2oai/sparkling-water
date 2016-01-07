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

package org.apache.spark.repl.h2o.commons

import org.apache.spark.SparkContext
import org.apache.spark.repl.Main
import org.apache.spark.repl.h2o.H2OInterpreter

/**
  * Various helper methods for working with REPL
  */
object InterpreterHelper {

  /**
    * Return class server output directory of REPL Class server.
    * @return
    */
  def classOutputDir = {
    if (Main.interp != null) {
      // Application was started using SparkSubmit
      Main.interp.intp.getClassOutputDirectory
    } else {
      ClassServer.getClassOutputDirectory
    }
  }


  /**
    * Return class server uri for REPL Class server.
    * In local mode the class server is not actually used, all we need is just output directory
    * @return
    */
  def classServerUri = {
    if (Main.interp != null) {
      // Application was started using SparkSubmit
      Main.interp.intp.classServerUri
    } else {
      if (!ClassServer.isRunning) {
        ClassServer.start()
      }
      ClassServer.classServerUri
    }
  }

  var sharedReplConfig: SharedReplConfig = _
  def initReplConfig(sc: SparkContext) = {
    sharedReplConfig = new SharedReplConfig(sc)
  }

  def createInterpreter(sesionID: Int): H2OInterpreter ={
    if(sharedReplConfig == null){
      throw new RuntimeException("Shared Repl configuration needs to be initialized before creating interpreter")
    }
    new H2OInterpreter(sharedReplConfig, sesionID)
  }
}
