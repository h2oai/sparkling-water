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

package ai.h2o.sparkling.backend.api.scalainterpreter

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.repl.H2OInterpreter
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap

/**
  * This servlet class handles requests for /3/scalaint endpoint
  */
private[api] class ScalaInterpreterServlet(conf: H2OConf) extends ServletBase with RestCommunication {

  private val intrPoolSize = conf.scalaIntDefaultNum
  private val freeInterpreters = new java.util.concurrent.ConcurrentLinkedQueue[H2OInterpreter]
  private var mapIntr = new TrieMap[Int, H2OInterpreter]
  private val lastIdUsed = new AtomicInteger(0)
  private val jobResults = new TrieMap[String, ScalaCodeResult]
  private val threadPoolMaxSize =
    if (conf.maxParallelScalaCellJobs == -1) Integer.MAX_VALUE else conf.maxParallelScalaCellJobs
  private val threadPool =
    new ThreadPoolExecutor(0, threadPoolMaxSize, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable])
  initializeInterpreterPool()

  def getSessions(): ScalaSessions = ScalaSessions(mapIntr.keys.toArray)

  def interpret(sessionId: Int, code: String): ScalaCode = {
    new ScalaInterpretJob(mapIntr(sessionId), conf, code, jobResults, threadPool).run()
  }

  def initSession(): ScalaSessionId = {
    val intp = fetchInterpreter()
    ScalaSessionId(intp.sessionId, conf.flowScalaCellAsync)
  }

  def getScalaCodeResult(resultKey: String): ScalaCode = {
    val result = jobResults(resultKey)
    ScalaCode(0, result.code, resultKey, result.scalaStatus, result.scalaResponse, result.scalaOutput, null)
  }

  def destroySession(sessionId: Int): ScalaSessionId = {
    mapIntr(sessionId).closeInterpreter()
    mapIntr -= sessionId
    ScalaSessionId(sessionId)
  }

  private def fetchInterpreter(): H2OInterpreter = {
    this.synchronized {
      if (!freeInterpreters.isEmpty) {
        val intp = freeInterpreters.poll()
        mapIntr.put(intp.sessionId, intp)
        new Thread(new Runnable {
          def run(): Unit = {
            createInterpreterInPool()
          }
        }).start()
        intp
      } else {
        // pool is empty at the moment and is being filled, return new interpreter without using the pool
        val id = lastIdUsed.incrementAndGet()
        val intp = new H2OInterpreter(SparkSessionUtils.active.sparkContext, id)
        mapIntr.put(intp.sessionId, intp)
        intp
      }
    }
  }

  private def initializeInterpreterPool(): Unit = {
    for (_ <- 0 until intrPoolSize) {
      createInterpreterInPool()
    }
  }

  private def createInterpreterInPool(): H2OInterpreter = {
    val id = lastIdUsed.incrementAndGet()
    val intp = new H2OInterpreter(SparkSessionUtils.active.sparkContext, id)
    freeInterpreters.add(intp)
    intp
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null => getSessions()
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null => initSession()
      case s if s.matches("/result/.*") =>
        val parameters = ScalaCodeResult.ScalaCodeResultParameters.parse(req)
        parameters.validate()
        getScalaCodeResult(parameters.resultKey)
      case s if s.matches("/.*") =>
        val parameters = ScalaCode.ScalaCodeParameters.parse(req)
        parameters.validate(mapIntr)
        interpret(parameters.sessionId, parameters.code)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case s if s.matches("/.*") =>
        val parameters = ScalaSessionId.ScalaSessionIdParameters.parse(req)
        parameters.validate(mapIntr)
        destroySession(parameters.sessionId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }
}

object ScalaInterpreterServlet extends ServletRegister {
  override protected def getRequestPaths(): Array[String] = Array("/3/scalaint", "/3/scalaint/*")

  override protected def getServlet(conf: H2OConf): Servlet = new ScalaInterpreterServlet(conf)
}
