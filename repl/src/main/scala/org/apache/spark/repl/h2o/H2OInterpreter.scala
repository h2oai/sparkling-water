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

import java.net.URI

import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext}

import scala.Predef.{println => _, _}
import scala.annotation.tailrec
import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.tools.nsc._
import scala.tools.nsc.interpreter.{Results => IR, _}
import scala.tools.nsc.util._

/**
  * H2O Interpreter which is use to interpret scala code
  * @param sparkContext spark context
  * @param sessionId session ID for interpreter
  */
class H2OInterpreter(val sparkContext: SparkContext, var sessionId: Int) extends Logging {

  private val ContinueString = "     | "
  private val consoleStream = new IntpConsoleStream()
  private val responseWriter = new IntpResponseWriter()
  private var replExecutionStatus = CodeResults.Success
  private var settings: Settings = _
  private var intp: H2OIMain = _
  private var in: InteractiveReader = _
  private[repl] var pendingThunks: List[() => Unit] = Nil

  def closeInterpreter() {
    if (intp ne null) {
      intp.reporter.flush()
    }
  }

  def valueOfTerm(term: String): Option[AnyRef] = {
    intp.valueOfTerm(term)
  }

  /**
    * Get response of interpreter
    * @return
    */
  def interpreterResponse: String = {
    responseWriter.content
  }

  /**
    * Redirected printed output coming from commands written in the interpreter
    * @return
    */
  def consoleOutput: String = {
    consoleStream.content
  }

  /**
    * Run scala code in a string
    * @param code Code to be compiled end executed
    * @return
    */
  def runCode(code: String): CodeResults.Value = {
    initBeforeRunningCode(code)
    // Redirect output from console to our own stream
    scala.Console.withOut(consoleStream) {
      try loop()
      catch AbstractOrMissingHandler()
    }

    if (exceptionOccurred()) {
      CodeResults.Exception
    } else {
      replExecutionStatus
    }
  }

  private def initializeInterpreter(): Unit = {
    if (sparkContext.master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")
    settings = createSettings()
    intp = createInterpreter()
    addThunk(
      intp.beQuietDuring{
        intp.bind("sc","org.apache.spark.SparkContext", sparkContext ,List("@transient"))
        intp.bind("sqlContext","org.apache.spark.sql.SQLContext",SQLContext.getOrCreate(sparkContext),List("@transient","implicit"))

        command(
          """
            @transient val h2oContext = {
              val _h2oContext = org.apache.spark.h2o.H2OContext.getOrCreate(sc)
              _h2oContext
            }
          """)

        intp.addImports(
          "org.apache.spark.SparkContext._",
          "org.apache.spark.sql.{DataFrame, Row, SQLContext}",
          "sqlContext.implicits._",
          "sqlContext.sql",
          "org.apache.spark.sql.functions._",
          "org.apache.spark.h2o._",
          "org.apache.spark._")
      })

    if (intp.reporter.hasErrors){
      throw new RuntimeException("Could not initialize the interpreter")
    }

    intp.initializeSynchronous()
    postInitialization()
  }

  private def createInterpreter(): H2OIMain = {
    val addedJars =
      if (Utils.isWindows) {
        // Strip any URI scheme prefix so we can add the correct path to the classpath
        // e.g. file:/C:/my/path.jar -> C:/my/path.jar
        SparkILoop.getAddedJars.map { jar => new URI(jar).getPath.stripPrefix("/") }
      } else {
        // We need new URI(jar).getPath here for the case that `jar` includes encoded white space (%20).
        SparkILoop.getAddedJars.map { jar => new URI(jar).getPath }
      }
    // work around for Scala bug
    val totalClassPath = addedJars.foldLeft(
      settings.classpath.value)((l, r) => ClassPath.join(l, r))
    this.settings.classpath.value = totalClassPath
    H2OIMain.createInterpreter(sparkContext, settings, responseWriter,sessionId)
  }

  /**
    * Initialize the compiler settings
    * @return
    */
  private def createSettings(): Settings = {
    val settings = new Settings()
    settings.usejavacp.value = true
    val loader = classTag[H2OInterpreter].runtimeClass.getClassLoader
    // Check if app.class.path resource on given classloader is set. In case it exists, set it as classpath
    // ( instead of using java class path right away)
    // This solves problem explained here: https://gist.github.com/harrah/404272
    val method = settings.getClass.getSuperclass.getDeclaredMethod("getClasspath",classOf[String],classOf[ClassLoader])
    method.setAccessible(true)
    if(method.invoke(settings, "app",loader).asInstanceOf[Option[String]].isDefined){
      settings.usejavacp.value = false
      settings.embeddedDefaults(loader)
    }

    // synchronous calls
    settings.Yreplsync.value = true

    for (jar <- sparkContext.addedJars) {
      settings.bootclasspath.append(jar._1)
      settings.classpath.append(jar._1)
    }
    settings
  }

  /**
    * Run all thunks after the interpreter has been initialized and throw exception if anything went wrong
    */
  private[repl] def postInitialization() {
    try {
      runThunks()
    } catch {
      case ex: Throwable => throw ex
    }
  }

  private[repl] def runThunks(): Unit = synchronized {
    if (pendingThunks.nonEmpty){
      logDebug("Clearing " + pendingThunks.size + " thunks.")
    }

    while (pendingThunks.nonEmpty) {
      val thunk = pendingThunks.head
      pendingThunks = pendingThunks.tail
      thunk()
    }
  }

  private[repl] def addThunk(body: => Unit) = synchronized {
    pendingThunks :+= (() => body)
  }

  private def exceptionOccurred(): Boolean = {
    intp.valueOfTerm("lastException").isDefined && intp.valueOfTerm("lastException").get != null
  }

  private def setSuccess() = {
    // Allow going to Success only from Incomplete
    if (replExecutionStatus == CodeResults.Incomplete) {
      replExecutionStatus = CodeResults.Success
    }
  }

  private def setIncomplete() = {
    // Allow going to Incomplete only from Success
    if (replExecutionStatus == CodeResults.Success) {
      replExecutionStatus = CodeResults.Incomplete
    }

  }

  private def setError() = {
    replExecutionStatus = CodeResults.Error
  }

  private def initBeforeRunningCode(code: String): Unit = {
    // reset variables
    replExecutionStatus = CodeResults.Success
    intp.beQuietDuring {
      command("val lastException: Throwable = null")
    }
    consoleStream.reset()
    responseWriter.reset()
    // set the input stream
    import java.io.{BufferedReader, StringReader}
    val input = new BufferedReader(new StringReader(code))
    in = SimpleReader(input, responseWriter, interactive = false)
  }

  /** The main read-eval-print loop for the repl.  It calls
    * command() for each line of input, and stops when
    * command() returns false.
    */
  private def loop() {
    def readOneLine() = {
      responseWriter.flush()
      in readLine ""
    }
    // return false if repl should exit
    def processLine(line: String): Boolean = {
      if (line eq null) false // assume null means EOF
      else command(line)
    }
    def innerLoop() {
      val shouldContinue = try {
        processLine(readOneLine())
      } catch {
        case t: Throwable =>
          throw t
      }
      if (shouldContinue){
        innerLoop()
      }
    }
    innerLoop()
  }

  private[repl] def echo(msg: String) = {
    responseWriter.print(msg)
    responseWriter.flush()
  }

  /** Run one command submitted by the user.  Two values are returned:
    * (1) whether to keep running, (2) the line to record for replay,
    * if any. */
  private[repl] def command(line: String): Boolean = {
    if (intp.global == null) false // Notice failure to create compiler
    else {
      interpretStartingWith(line)
      true
    }
  }

  /** Interpret expressions starting with the first line.
    * Read lines until a complete compilation unit is available
    * or until a syntax error has been seen.  If a full unit is
    * read, go ahead and interpret it.  Return the full string
    * to be recorded for replay, if any.
    */
  @tailrec
  private def interpretStartingWith(code: String): Unit = {

    val reallyResult = intp.interpret(code)
    reallyResult match {
      case IR.Error =>
        setError()
      case IR.Success =>
        setSuccess()
      case IR.Incomplete =>

        in.readLine(ContinueString) match {
          case null =>
            // we know compilation is going to fail since we're at EOF and the
            // parser thinks the input is still incomplete, but since this is
            // a file being read non-interactively we want to fail.  So we send
            // it straight to the compiler for the nice error message.

            // the interpreter thinks the code is incomplete, but it does not have to be exactly true
            // we try to compile the code, if it's ok, the code is correct, otherwise is really incomplete
            if (intp.compileString(code)) {
              setSuccess()
            } else {
              setIncomplete()
            }

          case line =>
            interpretStartingWith(code + "\n" + line)
        }
    }
  }

  initializeInterpreter()
}

object H2OInterpreter{
  /**
    * Return class server output directory of REPL Class server.
 *
    * @return
    */
  def classOutputDir = {
    if (org.apache.spark.repl.Main.interp != null) {
      // Application was started using SparkSubmit
      org.apache.spark.repl.Main.interp.intp.getClassOutputDirectory
    } else {
      REPLClassServer.getClassOutputDirectory
    }
  }


  /**
    * Return class server uri for REPL Class server.
    * In local mode the class server is not actually used, all we need is just output directory
 *
    * @return
    */
  def classServerUri = {
    if (org.apache.spark.repl.Main.interp != null) {
      // Application was started using SparkSubmit
      org.apache.spark.repl.Main.interp.intp.classServerUri
    } else {
      REPLClassServer.classServerUri
    }
  }
}
