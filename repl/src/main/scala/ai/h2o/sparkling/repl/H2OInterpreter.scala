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
package ai.h2o.sparkling.repl

import java.io.File
import java.net.URI
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.tools.nsc._

/**
  * H2O Interpreter which is use to interpret scala code
  *
  * @param sparkContext spark context
  * @param hc           H2OContext
  * @param sessionId    session ID for interpreter
  */
class H2OInterpreter(sparkContext: SparkContext, hc: Any, sessionId: Int)
  extends BaseH2OInterpreter(sparkContext, hc, sessionId) {

  override def createInterpreter(): H2OIMain = {
    H2OIMain.createInterpreter(sparkContext, settings, responseWriter, sessionId)
  }

  override def createSettings(): Settings = {
    val result = new Settings(echo)

    // Check if app.class.path resource on given classloader is set. In case it exists, set it as classpath
    // ( instead of using java class path right away)
    // This solves problem explained here: https://gist.github.com/harrah/404272
    val classLoader = classTag[H2OInterpreter].runtimeClass.getClassLoader
    val isAppClassPathSet = getClassLoaderAppClassPath(classLoader).isDefined
    if (isAppClassPathSet) result.embeddedDefaults(classLoader)
    val useJavaCpArg = if (!isAppClassPathSet) Some("-usejavacp") else None

    val conf = sparkContext.getConf
    val jars = getJarsForShell(conf)

    val interpArguments = List(
      "-Yrepl-sync", // prevent each repl line from being run in a new thread
      "-Yrepl-class-based", // ensure that lines in REPL are wrapped in the classes instead of objects
      "-Yrepl-outdir",
      s"${H2OInterpreter.classOutputDirectory.getAbsolutePath}",
      "-classpath",
      jars) ++ useJavaCpArg

    result.processArguments(interpArguments, processAll = true)

    result
  }

  private def getClassLoaderAppClassPath(runtimeClassLoader: ClassLoader): Option[String] =
    for {
      classLoader <- Option(runtimeClassLoader)
      appClassPathResource <- Option(classLoader.getResource("app.class.path"))
      appClassPath = Source.fromURL(appClassPathResource)
    } yield appClassPath.mkString

  private def getJarsForShell(conf: SparkConf): String = {
    val localJars = conf.getOption("spark.repl.local.jars")
    val jarPaths = localJars.getOrElse("").split(",")
    jarPaths
      .map { path =>
        // Remove file:///, file:// or file:/ scheme if exists for each jar
        if (path.startsWith("file:")) new File(new URI(path)).getPath else path
      }
      .mkString(File.pathSeparator)
  }
}

object H2OInterpreter {
  def classOutputDirectory: File = H2OIMain.classOutputDirectory
}
