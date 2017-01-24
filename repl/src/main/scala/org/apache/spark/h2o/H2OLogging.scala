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
package org.apache.spark.h2o

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.slf4j.impl.StaticLoggerBinder

/**
  * This is a copy/paste of https://github.com/cloudera/spark/blob/master/core/src/main/scala/org/apache/spark/Logging.scala
  * Cloudera's 1.6.0-cdh5.9 Spark changed some of the Logging methods to have isInterpreter as an argument, without them if
  * we compile against vanilla Spark 1.6.0 we'll get runtime AbstractMethodException exceptions.
  */
trait H2OLogging extends Logging {
  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    if (!H2OLogging.initialized) {
      Logging.initLock.synchronized {
        if (!H2OLogging.initialized) {
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  protected def initializeLogging(isInterpreter: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val rootLogger = LogManager.getRootLogger()
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          System.err.println("To adjust logging level use sc.setLogLevel(newLevel).")
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    H2OLogging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private object H2OLogging {
  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
