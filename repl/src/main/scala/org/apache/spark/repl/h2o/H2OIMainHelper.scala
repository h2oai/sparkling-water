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

import org.apache.spark.repl.Main
import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import scala.tools.nsc.interpreter.Naming

/**
  * Helper methods for H2OIMain on both scala versions
  */
trait H2OIMainHelper {

  private var _initialized = false

  /**
    * Ensure that each class defined in REPL is in a package containing number of repl session
    */
  def setupClassNames(naming: Naming, sessionId: Int): Unit = {
    import naming._
    // sessionNames is lazy val and needs to be accessed first in order to be then set again to our desired value
    naming.sessionNames.line
    val fieldSessionNames = naming.getClass.getDeclaredField("sessionNames")
    fieldSessionNames.setAccessible(true)
    fieldSessionNames.set(naming, new SessionNames {
      override def line  = "intp_id_" + sessionId + propOr("line")
    })
  }

  def setClassLoaderToSerializers(classLoader: ClassLoader): Unit = {
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  def newREPLDirectory(): File = {
    val conf = new SparkConf()
    val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
    val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
    outputDir
  }

  private def prepareLocalClassLoader() = {
    val f = SparkEnv.get.serializer.getClass.getSuperclass.getDeclaredField("defaultClassLoader")
    f.setAccessible(true)
    val value = f.get(SparkEnv.get.serializer)
    value match {
      case v: Option[_] => {
        v.get match {
          case cl: MutableURLClassLoader => cl.addURL(H2OInterpreter.classOutputDirectory.toURI.toURL)
          case _ =>
        }
      }
      case _ =>
    }
  }

  def initializeClassLoader(sc: SparkContext): Unit = {
    if(!_initialized){
      if (sc.isLocal) {
        prepareLocalClassLoader()
        setClassLoaderToSerializers(new InterpreterClassLoader(Thread.currentThread.getContextClassLoader))
      } else if (Main.interp != null) {
        // Application has been started using SparkShell script.
        // Set the original interpreter classloader as the fallback class loader for all
        // class not defined in our custom REPL.
        setClassLoaderToSerializers(new InterpreterClassLoader(Main.interp.intp.classLoader))
      } else {
        // Application hasn't been started using SparkShell.
        // Set the context classloader as the fallback class loader for all
        // class not defined in our custom REPL
        setClassLoaderToSerializers(new InterpreterClassLoader(Thread.currentThread.getContextClassLoader))
      }
      _initialized = true
    }
  }
}
