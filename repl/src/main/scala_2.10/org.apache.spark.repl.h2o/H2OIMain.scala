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

import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.spark.{HttpServer, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.repl.{Main, SparkIMain}

import scala.collection.mutable
import scala.reflect.io.PlainFile
import scala.tools.nsc.Settings

/**
  * SparkIMain allowing multiple interpreters to coexist in parallel. Each line in repl is wrapped in a package which
  * name contains current session id
  */
private[repl] class H2OIMain private(initialSettings: Settings,
                                     interpreterWriter: IntpResponseWriter,
                                     val sessionID: Int,
                                     propagateExceptions: Boolean = false)
  extends SparkIMain(initialSettings, interpreterWriter, propagateExceptions) {


  // we need to setup the compiler and stop the class server only when spark version detected at runtime doesn't actually
  // use dedicated repl server, se we handle the output directory in the same way as on Spark 2.0
  if(BaseH2OInterpreter.classServerFieldAvailable) {
    setupCompiler()
    stopClassServer()
  }
  setupClassNames()


  /**
    * Stop class server started in SparkIMain constructor because we have to use our class server which is already
    * running
    */
  private def stopClassServer() = {
      val fieldClassServer = this.getClass.getSuperclass.getDeclaredField("classServer")
      fieldClassServer.setAccessible(true)
      val classServer = fieldClassServer.get(this).asInstanceOf[HttpServer]
      classServer.stop()
  }

  /**
    * Create a new instance of compiler with the desired output directory
    */
  private def setupCompiler() = {
    // set virtualDirectory to our shared directory for all repl instances
    val fieldVirtualDirectory = this.getClass.getSuperclass.getDeclaredField("org$apache$spark$repl$SparkIMain$$virtualDirectory")
    fieldVirtualDirectory.setAccessible(true)
    fieldVirtualDirectory.set(this, new PlainFile(H2OIMain.classOutputDirectory))

    // initialize the compiler again with new virtualDirectory set
    val fieldCompiler = this.getClass.getSuperclass.getDeclaredField("_compiler")
    fieldCompiler.setAccessible(true)
    fieldCompiler.set(this, newCompiler(settings, reporter))
  }

  /**
    * Ensure that each class defined in repl is in a package containing number of repl session
    */
  private def setupClassNames() = {
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

  /**
    * Add directory with classes defined in repl to the classloader
    * which is used in the local mode. This classloader is obtained using reflections.
    */
  private def prepareLocalClassLoader() = {
    val f = SparkEnv.get.serializer.getClass.getSuperclass.getDeclaredField("defaultClassLoader")
    f.setAccessible(true)
    val value = f.get(SparkEnv.get.serializer)
    value match {
      case v: Option[_] => {
        v.get match {
          case cl: MutableURLClassLoader => cl.addURL(H2OIMain.classOutputDirectory.toURI.toURL)
          case _ =>
        }
      }
      case _ =>
    }
  }

  private def initialize(sc: SparkContext): Unit = {
    if (sc.isLocal) {
      // master set to local or local[*]

      prepareLocalClassLoader()
      interpreterClassloader = new InterpreterClassLoader(Thread.currentThread.getContextClassLoader)
    } else {
      if (Main.interp != null) {
        interpreterClassloader = new InterpreterClassLoader(Main.interp.intp.classLoader)
      } else {
        // non local mode, application not started using SparkSubmit
        interpreterClassloader = new InterpreterClassLoader(Thread.currentThread.getContextClassLoader)
      }
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

  // this is used only in case the spark version detected at runtime doesn't actually use dedicated repl server,
  // se we handle the output directory in the same way as on Spark 2.0
  private lazy val _classOutputDirectory = {
    if (Main.interp != null) {
      // Application was started using shell, we can reuse this directory
      Main.interp.intp.getClassOutputDirectory
    } else if(BaseH2OInterpreter.classServerFieldAvailable){
        // in case the class server field is available we need to return class output directory of the class server
        REPLClassServer.getClassOutputDirectory
    } else {
      // otherwise the field is not available, which means that this spark version is no longer using dedicated
      // repl server and we handle this as in Spark 2.0
      // REPL hasn't been started yet, create a new directory
      val conf = new SparkConf()
      val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
      val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
      outputDir
    }
  }

  def classOutputDirectory =  _classOutputDirectory
}