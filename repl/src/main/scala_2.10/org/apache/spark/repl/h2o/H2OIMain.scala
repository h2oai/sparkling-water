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

/**
  * SparkIMain allowing multiple interpreters to coexist in parallel. Each line in repl is wrapped in a package which
  * name contains current session id
  */
private[repl] class H2OIMain private(initialSettings: Settings,
                                     interpreterWriter: IntpResponseWriter,
                                     val sessionID: Int,
                                     propagateExceptions: Boolean = false) extends{
  override private[repl] val outputDir: File = H2OIMain.classOutputDirectory
} with SparkIMain(initialSettings, interpreterWriter, propagateExceptions) {

  setupClassNames()

  /**
    * Ensure that each class defined in repl is in a package containing number of repl session
    */
  def setupClassNames() = {
    import naming._
    // sessionNames is lazy val and needs to be accessed first in order to be then set again to our desired value
    naming.sessionNames.line
    val fieldSessionNames = naming.getClass.getDeclaredField("sessionNames")
    fieldSessionNames.setAccessible(true)
    fieldSessionNames.set(naming, new SessionNames {
      override def line  = "intp_id_" + sessionID + "." + propOr("line")
    })
  }
}


object H2OIMain {

  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]
  private var interpreterClassloader: InterpreterClassLoader = _
  private var _initialized = false

  private def setClassLoaderToSerializers(classLoader: ClassLoader): Unit = {
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  private def initialize(sc: SparkContext): Unit = {
    if (Main.interp != null) {
      // Application has been started using SparkShell script.
      // Set the original interpreter classloader as the fallback class loader for all
      // class not defined in our custom REPL.
      interpreterClassloader = new InterpreterClassLoader(Main.interp.intp.classLoader)
    } else {
      // Application hasn't been started using SparkShell.
      // Set the context classloader as the fallback class loader for all
      // class not defined in our custom REPL
      interpreterClassloader = new InterpreterClassLoader(Thread.currentThread.getContextClassLoader)
    }
    setClassLoaderToSerializers(interpreterClassloader)
  }

  def getInterpreterClassloader: InterpreterClassLoader = {
    interpreterClassloader
  }


  def createInterpreter(sc: SparkContext, settings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int): H2OIMain = synchronized {
    if(!_initialized){
      initialize(sc)
      _initialized = true
    }
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId, false))
    existingInterpreters(sessionId)
  }

  private lazy val _classOutputDirectory = {
    if (Main.interp != null) {
      // Application was started using shell, we can reuse this directory
     Main.interp.intp.getClassOutputDirectory
    } else {
      // REPL hasn't been started yet, create a new directory
      val conf = new SparkConf()
      val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
      val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
      outputDir
    }
  }

  def classOutputDirectory = _classOutputDirectory
}