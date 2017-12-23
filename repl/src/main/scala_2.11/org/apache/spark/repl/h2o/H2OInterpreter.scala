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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.Utils

import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.tools.nsc._


/**
  * H2O Interpreter which is use to interpret scala code
  *
  * @param sparkContext spark context
  * @param sessionId session ID for interpreter
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

    val jars = H2OInterpreter.getUserJars(conf).mkString(File.pathSeparator)

    val interpArguments = List(
      "-Yrepl-class-based", // ensure that lines in REPL are wrapped in the classes instead of objects
      "-Yrepl-outdir", s"${H2OInterpreter.classOutputDirectory.getAbsolutePath}",
      "-classpath", jars
    )

    settings.processArguments(interpArguments, processAll = true)

    settings
  }


  override def valueOfTerm(term: String): Option[Any] = {
    try Some(intp.eval(term))  catch { case _ : Exception => None }
  }
}

object H2OInterpreter {
  def classOutputDirectory = H2OIMain.classOutputDirectory

  def getUserJars(conf: SparkConf): Seq[String] = {
    import scala.reflect.runtime.{universe => ru}
    val instanceMirror = ru.runtimeMirror(this.getClass.getClassLoader).reflect(Utils)
    val methodSymbol = ru.typeOf[Utils.type].decl(ru.TermName("getLocalUserJarsForShell"))
    if (methodSymbol.isMethod) {
      val method = instanceMirror.reflectMethod(methodSymbol.asMethod)
      method(conf).asInstanceOf[Seq[String]]
    } else {
      // Fallback to Spark 2.2.0
      val m = ru.runtimeMirror(this.getClass.getClassLoader)
      val instanceMirror = m.reflect(Utils)
      val methodSymbol = ru.typeOf[Utils.type].decl(ru.TermName("getUserJars")).asMethod
      val method = instanceMirror.reflectMethod(methodSymbol)
      method(conf, true).asInstanceOf[Seq[String]]
    }
  }
}