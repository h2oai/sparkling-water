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
  * Link on Github: https://github.com/apache/spark/blob/master/repl/scala-2.10/src/main/scala/org/apache/spark/repl/SparkILoop.scala
  * Author: Alexander Spoon
  */

package org.apache.spark.repl.h2o


import java.io.File
import java.net.URI

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.Utils

import scala.Predef.{println => _, _}
import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.tools.nsc._
import scala.tools.nsc.util._

/**
  * H2O Interpreter which is use to interpret scala code
 *
  * @param sparkContext spark context
  * @param sessionId session ID for interpreter
  */
class H2OInterpreter(sparkContext: SparkContext, sessionId: Int) extends BaseH2OInterpreter(sparkContext, sessionId) {


  private def getAddedJars(): Array[String] = {
    val conf = sparkContext.getConf
    val envJars = sys.env.get("ADD_JARS")
    if (envJars.isDefined) {
      logWarning("ADD_JARS environment variable is deprecated, use --jar spark submit argument instead")
    }
    val jars = {
      val userJars = H2OInterpreter.getUserJars(conf).mkString(File.pathSeparator)
      if (userJars.isEmpty) {
        envJars.getOrElse("")
      } else {
        userJars.mkString(",")
      }
    }
    Utils.resolveURIs(jars).split(",").filter(_.nonEmpty)
  }

  override def createInterpreter(): H2OIMain = {
    val addedJars =
      if (Utils.isWindows) {
        // Strip any URI scheme prefix so we can add the correct path to the classpath
        // e.g. file:/C:/my/path.jar -> C:/my/path.jar
        getAddedJars().map { jar => new URI(jar).getPath.stripPrefix("/") }
      } else {
        // We need new URI(jar).getPath here for the case that `jar` includes encoded white space (%20).
        getAddedJars().map { jar => new URI(jar).getPath }
      }
    // work around for Scala bug
    val totalClassPath = addedJars.foldLeft(
      settings.classpath.value)((l, r) => ClassPath.join(l, r))
    this.settings.classpath.value = totalClassPath
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
    val method = settings.getClass.getSuperclass.getDeclaredMethod("getClasspath",classOf[String],classOf[ClassLoader])
    method.setAccessible(true)
    if(method.invoke(settings, "app",loader).asInstanceOf[Option[String]].isDefined){
      settings.usejavacp.value = false
      settings.embeddedDefaults(loader)
    }

    settings
  }
}

object H2OInterpreter {
  def classOutputDirectory = H2OIMain.classOutputDirectory


  def getUserJars(conf: SparkConf): Seq[String] = {
    import scala.reflect.runtime.{universe => ru}

    val instanceMirror = ru.runtimeMirror(this.getClass.getClassLoader).reflect(Utils)
    val methodSymbol = ru.typeOf[Utils.type].declaration(ru.stringToTermName("getLocalUserJarsForShell"))
    if (methodSymbol.isMethod) {
      val method = instanceMirror.reflectMethod(methodSymbol.asMethod)
      method(conf).asInstanceOf[Seq[String]]
    } else {
      // Fallback to Spark 2.2.0
      val m = ru.runtimeMirror(this.getClass.getClassLoader)
      val instanceMirror = m.reflect(Utils)
      val methodSymbol = ru.typeOf[Utils.type].declaration(ru.stringToTermName("getUserJars")).asMethod
      val method = instanceMirror.reflectMethod(methodSymbol)
      method(conf, true).asInstanceOf[Seq[String]]
    }
  }
}
