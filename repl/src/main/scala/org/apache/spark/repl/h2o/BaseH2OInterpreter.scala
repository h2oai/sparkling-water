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


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

import scala.Predef.{println => _}
import scala.annotation.tailrec
import scala.language.{existentials, implicitConversions, postfixOps}
import scala.tools.nsc._
import scala.tools.nsc.interpreter.{Results => IR, _}

/**
  * H2O Interpreter which is use to interpret scala code. This class is base class for H2O Interpreter in scala
  * 2.10 and 2.11
  *
  * @param sparkContext spark context
  * @param sessionId session ID for interpreter
  */
private[repl] abstract class BaseH2OInterpreter(val sparkContext: SparkContext, var sessionId: Int) extends Logging {

  private val ContinueString = "     | "
  private val consoleStream = new IntpConsoleStream()
  protected val responseWriter = new IntpResponseWriter()
  private var replExecutionStatus = CodeResults.Success
  protected var settings: Settings = _
  protected var intp: H2OIMain = _
  private var in: InteractiveReader = _
  private[repl] var pendingThunks: List[() => Unit] = Nil

  def closeInterpreter() {
    if (intp ne null) {
      intp.reporter.flush()
    }
  }

  def valueOfTerm(term: String): Option[Any] = {
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
  def runCode(code: String): CodeResults.Value = BaseH2OInterpreter.savingContextClassloader {
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
    settings = createSettings()
    intp = createInterpreter()
    val spark = SparkSession.builder().getOrCreate()
    addThunk(
      intp.beQuietDuring{
        intp.bind("sc", "org.apache.spark.SparkContext", sparkContext, List("@transient"))
        intp.bind("spark", "org.apache.spark.sql.SparkSession", spark, List("@transient"))
        intp.bind("sqlContext", "org.apache.spark.sql.SQLContext", spark.sqlContext, List("@transient", "implicit"))

        command(
          """
            @transient val h2oContext = {
              val _h2oContext = org.apache.spark.h2o.H2OContext.get().getOrElse(throw new RuntimeException(
              "H2OContext has to be started in order to use H2O REPL"))
              _h2oContext
            }
          """)

        command("import org.apache.spark.SparkContext._")
        command("import org.apache.spark.sql.{DataFrame, Row, SQLContext}")
        command("import sqlContext.implicits._")
        command("import sqlContext.sql")
        command("import org.apache.spark.sql._")
        command("import org.apache.spark.sql.functions._")
        command("import org.apache.spark.h2o._")
        command("import org.apache.spark._")

      })

    if (intp.reporter.hasErrors){
      throw new RuntimeException("Could not initialize the interpreter")
    }

    intp.initializeSynchronous()
    postInitialization()
  }

  protected def createInterpreter(): H2OIMain

  /**
    * Initialize the compiler settings
    */
  protected def createSettings(): Settings

  /**
    * Run all thunks after the interpreter has been initialized and throw exception if anything went wrong
    */
  private[repl] def postInitialization() : Unit = BaseH2OInterpreter.savingContextClassloader {
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
    valueOfTerm("lastException").isDefined && valueOfTerm("lastException").get != null
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

object BaseH2OInterpreter {

  def savingContextClassloader[T](body: => T): T = {
    val classloader = Thread.currentThread().getContextClassLoader
    try {
      body
    }
    finally Thread.currentThread().setContextClassLoader(classloader)
  }

}
