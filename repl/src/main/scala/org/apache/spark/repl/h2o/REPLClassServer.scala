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
/**
  * This code is based on code org.apache.spark.repl.SparkIMain released under Apache 2.0"
  * Link to Github: https://github.com/apache/spark/blob/master/repl/scala-2.10/src/main/scala/org/apache/spark/repl/SparkIMain.scala
  * Author:  Paul Phillips
  */

package org.apache.spark.repl.h2o

import org.apache.spark.h2o.H2OLogging
import org.apache.spark.util.Utils
import org.apache.spark.{HttpServer, SecurityManager, SparkConf}


/**
  * HTTP Server storing classes defined in REPL
  */
private[repl] object REPLClassServer extends H2OLogging {

  lazy val getClassOutputDirectory = outputDir
  /** Local directory to save .class files too */
  private lazy val outputDir = {
    val tmp = System.getProperty("java.io.tmpdir")
    val rootDir = conf.get("spark.repl.classdir",  tmp)
    Utils.createTempDir(rootDir)
  }
  private val conf = new SparkConf()
  logInfo("Directory to save .class files to = " + outputDir)
  /** Jetty server that will serve our classes to worker nodes */
  private val classServerPort = conf.getInt("spark.replClassServer.port", 0)
  private val classServer = new HttpServer(conf, outputDir, new SecurityManager(conf), classServerPort, "HTTP class server")
  private var _isRunning = false

  def start() ={
    classServer.start()
    _isRunning = true
    logInfo("Class server started, URI = " + classServerUri)
  }

  def classServerUri: String  = synchronized {
    if(!_isRunning) {
      start()
    }
    classServer.uri
  }

  def close() {
    if(classServer != null){
      classServer.stop()
      classServer == null
    }
  }

  def isRunning: Boolean = {
    _isRunning
  }
}