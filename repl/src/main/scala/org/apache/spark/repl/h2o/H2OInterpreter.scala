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
  * This code is based on code org.apache.spark.repl.SparkILoop released under Apache 2.0"
  * Link on Github: https://github.com/apache/spark/blob/master/repl/scala-2.11/src/main/scala/org/apache/spark/repl/SparkILoop.scala
  * Author: Alexander Spoon
  */

package org.apache.spark.repl.h2o


import java.io.File
import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.util.Utils

import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.tools.nsc._


/**
  * H2O Interpreter which is use to interpret scala code
  *
  * @param sparkContext spark context
  * @param sessionId    session ID for interpreter
  */
class H2OInterpreter(sparkContext: SparkContext, sessionId: Int) extends BaseH2OInterpreter(sparkContext, sessionId) {

  override def createInterpreter(): H2OIMain = {
    H2OIMain.createInterpreter(sparkContext, settings, responseWriter, sessionId)
  }

  override def createSettings(): Settings = {
    val settings = new Settings(echo)
    // prevent each repl line from being run in a new thread
    settings.Yreplsync.value = true

    // Check if app.class.path resource on given classloader is set. In case it exists, set it as classpath
    // ( instead of using java class path right away)
    // This solves problem explained here: https://gist.github.com/harrah/404272
    settings.usejavacp.value = true
    val loader = classTag[H2OInterpreter].runtimeClass.getClassLoader
    val method = settings.getClass.getSuperclass.getDeclaredMethod("getClasspath", classOf[String], classOf[ClassLoader])
    method.setAccessible(true)
    if (method.invoke(settings, "app", loader).asInstanceOf[Option[String]].isDefined) {
      settings.usejavacp.value = false
      settings.embeddedDefaults(loader)
    }

    val conf = sparkContext.getConf

    val jars = Utils.getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)


    val interpArguments = List(
      "-Yrepl-class-based", // ensure that lines in REPL are wrapped in the classes instead of objects
      "-Yrepl-outdir", s"${H2OInterpreter.classOutputDirectory.getAbsolutePath}",
      "-classpath", jars
    )

    settings.processArguments(interpArguments, processAll = true)

    settings
  }


  override def valueOfTerm(term: String): Option[Any] = {
    try Some(intp.eval(term)) catch {
      case _: Exception => None
    }
  }
}

object H2OInterpreter {
  def classOutputDirectory = H2OIMain.classOutputDirectory
}
