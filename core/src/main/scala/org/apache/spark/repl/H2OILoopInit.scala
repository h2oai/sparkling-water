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


/*
 * ------------------------------------------------------------------------------------------------------
 *
 * This code is based on code org.apache.spark.repl.SparkILoopInit released under Apache 2.0"
 * Link on Github: https://github.com/apache/spark/blob/master/repl/scala-2.10/src/main/scala/org/apache/spark/repl/SparkILoopInit.scala
 * Author: Paul Phillips
 *
 * ------------------------------------------------------------------------------------------------------
 */

package org.apache.spark.repl

import org.apache.spark.SPARK_VERSION
import org.apache.spark.repl.H2OILoop
import org.apache.spark.sql.SQLContext

import scala.tools.nsc._
import scala.tools.nsc.interpreter._
import scala.tools.nsc.util.stackTraceString

// scalastyle:off
/**
 *  Machinery for the asynchronous initialization of the repl.
 */
private[repl] trait H2OILoopInit {
  self: H2OILoop =>

  private val initLock = new java.util.concurrent.locks.ReentrantLock()
  private val initCompilerCondition = initLock.newCondition() // signal the compiler is initialized
  private val initLoopCondition = initLock.newCondition()     // signal the whole repl is initialized
  private val initStart = System.nanoTime
  // a condition used to ensure serial access to the compiler.
  @volatile private var initIsComplete = false
  @volatile private var initError: String = null
  // code to be executed only after the interpreter is initialized
  // and the lazy val `global` can be accessed without risk of deadlock.
  private var pendingThunks: List[() => Unit] = Nil

  /** Print a welcome message */
  def printWelcome() {
    echo( """Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
          """.format(SPARK_VERSION))
    import Properties._
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }
org.apache.spark.repl.Main
  def initializeSpark() {
    intp.beQuietDuring {
      command("""
              @transient val sc = {
                val _sc = org.apache.spark.SparkContext.getOrCreate()
                _sc
              }
                    """)
      command("""
        @transient val sqlContext = {
          val _sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
          _sqlContext
        }
              """)

      command("""
        @transient val h2oContext = {
          val _h2oContext = org.apache.spark.h2o.H2OContext.getOrCreate(sc)
          _h2oContext
        }
              """)
      command("import org.apache.spark.SparkContext._")
      command("import org.apache.spark.sql.{DataFrame, Row, SQLContext}")
      command("import sqlContext.implicits._")
      command("import sqlContext.sql")
      command("import org.apache.spark.sql.functions._")
      command("import org.apache.spark.h2o._")
      command("import org.apache.spark._")
    }
  }

  // the method to be called when the interpreter is initialized.
  // Very important this method does nothing synchronous (i.e. do
  // not try to use the interpreter) because until it returns, the
  // repl's lazy val `global` is still locked.
  protected def initializedCallback() = withLock(initCompilerCondition.signal())

  private def withLock[T](body: => T): T = {
    initLock.lock()
    try body
    finally initLock.unlock()
  }

  // Spins off a thread which awaits a single message once the interpreter
  // has been initialized.
  protected def createAsyncListener() = {
    io.spawn {
      withLock(initCompilerCondition.await())
      asyncMessage("[info] compiler init time: " + elapsed() + " s.")
      postInitialization()
    }
  }

  // ++ (
  //   warningsThunks
  // )
  // called once after init condition is signalled
  protected def postInitialization() {
    try {
      postInitThunks foreach (f => addThunk(f()))
      runThunks()
    } catch {
      case ex: Throwable =>
        initError = stackTraceString(ex)
        throw ex
    } finally {
      initIsComplete = true

      if (isAsync) {
        asyncMessage("[info] total init time: " + elapsed() + " s.")
        withLock(initLoopCondition.signal())
      }
    }
  }
  // private def warningsThunks = List(
  //   () => intp.bind("lastWarnings", "" + typeTag[List[(Position, String)]], intp.lastWarnings _),
  // )

  protected def postInitThunks = List[Option[() => Unit]](
    Some(intp.setContextClassLoader _),
    if (isReplPower) Some(() => enablePowerMode(true)) else None
  ).flatten

  protected def addThunk(body: => Unit) = synchronized {
    pendingThunks :+= (() => body)
  }

  protected def runThunks(): Unit = synchronized {
    if (pendingThunks.nonEmpty)
      logDebug("Clearing " + pendingThunks.size + " thunks.")

    while (pendingThunks.nonEmpty) {
      val thunk = pendingThunks.head
      pendingThunks = pendingThunks.tail
      thunk()
    }
  }

  private def elapsed() = "%.3f".format((System.nanoTime - initStart).toDouble / 1000000000L)

  protected def asyncMessage(msg: String) {
    if (isReplInfo || isReplPower)
      echoAndRefresh(msg)
  }

  // called from main repl loop
  protected def awaitInitialized(): Boolean = {
    if (!initIsComplete)
      withLock {
        while (!initIsComplete) initLoopCondition.await()
      }
    if (initError != null) {
      println( """
                 |Failed to initialize the REPL due to an unexpected error.
                 |This is a bug, please, report it along with the error diagnostics printed below.
                 |%s.""".stripMargin.format(initError)
      )
      false
    } else true
  }
}
// scalastyle:on
