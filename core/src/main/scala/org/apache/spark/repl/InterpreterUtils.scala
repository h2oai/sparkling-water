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
package org.apache.spark.repl

import java.net.URL

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.util.MutableURLClassLoader

import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

/**
 * Various utils for working with REPL class server
 */


object REPLClassServerUtils {

  def getClassOutputDir = {
    if (Main.interp != null) {
      Main.interp.intp.getClassOutputDirectory
    } else {
      REPLCLassServer.getClassOutputDirectory
    }
  }

  def classServerUri = {
    if (Main.interp != null) {
      Main.interp.intp.classServerUri
    } else {
      if (!REPLCLassServer.isRunning) {
        REPLCLassServer.start()
      }
    REPLCLassServer.classServerUri
  }
  }

}

private[repl] trait ExposeAddUrl extends URLClassLoader {
  def addNewUrl(url: URL) = this.addURL(url)
}