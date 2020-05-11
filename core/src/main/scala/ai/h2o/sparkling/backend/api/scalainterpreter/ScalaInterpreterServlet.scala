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

import ai.h2o.sparkling.backend.api.ServletRegister
import ai.h2o.sparkling.extensions.rest.api.ServletBase
import ai.h2o.sparkling.repl.H2OInterpreter
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.Gson
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.spark.h2o.H2OContext
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder, ServletMapping}
import water.H2O.H2OCountedCompleter
import water.api.schemas3.JobV3
import water.exceptions.H2ONotFoundArgumentException
import water.server.ServletUtils
import water.{DKV, Job, Key}

import scala.collection.concurrent.TrieMap

/**
  * This servlet class handles requests for /3/scalaint endpoint
  */
private[api] class ScalaInterpreterServlet() extends ServletBase {

  private lazy val hc = H2OContext.ensure()
  private val intrPoolSize = hc.getConf.scalaIntDefaultNum
  private val freeInterpreters = new java.util.concurrent.ConcurrentLinkedQueue[H2OInterpreter]
  private var mapIntr = new TrieMap[Int, H2OInterpreter]
  private val lastIdUsed = new AtomicInteger(0)
  private val jobCount = new AtomicInteger(0)
  initializeInterpreterPool()

  def getSessions(): ScalaSessions = {
    val sessions = new ScalaSessions
    sessions.sessions = mapIntr.keys.toArray
    sessions
  }

  def interpret(version: Int, s: ScalaCodeV3): ScalaCodeV3 = {
    // check if session exists
    if (s.session_id == -1 || !mapIntr.isDefinedAt(s.session_id)) {
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }

    val job = new Job[ScalaCodeResult](Key.make[ScalaCodeResult](), classOf[ScalaCodeResult].getName, "ScalaCodeResult")

    this.synchronized {
      jobCount.incrementAndGet()
      while (hc.getConf.maxParallelScalaCellJobs != -1 && jobCount
               .intValue() > hc.getConf.maxParallelScalaCellJobs) {
        Thread.sleep(1000)
      }
    }

    job.start(new H2OCountedCompleter() {
      override def compute2(): Unit = {

        val scalaCodeExecution = new ScalaCodeResult(job._result)
        val intp = mapIntr(s.session_id)
        scalaCodeExecution.code = s.code
        scalaCodeExecution.scalaStatus = intp.runCode(s.code).toString
        scalaCodeExecution.scalaResponse = intp.interpreterResponse
        scalaCodeExecution.scalaOutput = intp.consoleOutput
        DKV.put(scalaCodeExecution)
        tryComplete()
        jobCount.decrementAndGet()
      }
    }, 1)

    s.job = new JobV3(job)

    /** If we are not running in asynchronous mode, compute the result right away */
    if (!hc.getConf.flowScalaCellAsync) {
      val result = job.get()
      s.status = result.scalaStatus
      s.response = result.scalaResponse
      s.output = result.scalaOutput
    }
    s
  }

  def initSession(version: Int, s: ScalaSessionIdV3): ScalaSessionIdV3 = {
    val intp = fetchInterpreter()
    s.session_id = intp.sessionId
    s.async = hc.getConf.flowScalaCellAsync
    s
  }

  def getScalaCodeResult(version: Int, s: ScalaCodeV3): ScalaCodeV3 = {
    val result = DKV.getGet[ScalaCodeResult](s.result_key)
    s.code = result.code
    s.status = result.scalaStatus
    s.response = result.scalaResponse
    s.output = result.scalaOutput
    s
  }

  def destroySession(version: Int, s: ScalaSessionIdV3): ScalaSessionIdV3 = {
    if (!mapIntr.contains(s.session_id)) {
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }
    mapIntr(s.session_id).closeInterpreter()
    mapIntr -= s.session_id
    s
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    processRequest(req, resp) {
      val path = req.getServletPath
      val obj = path match {
        case "/3/scalaint" =>
          getSessions()
      }
      val json = new Gson().toJson(obj)
      withResource(resp.getWriter) { writer =>
        resp.setContentType("application/json")
        resp.setCharacterEncoding("UTF-8")
        writer.print(json)
      }
      ServletUtils.setResponseStatus(resp, HttpServletResponse.SC_OK)
    }
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
        val intp = new H2OInterpreter(hc.sparkContext, id)
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
    val intp = new H2OInterpreter(hc.sparkContext, id)
    freeInterpreters.add(intp)
    intp
  }
}

object ScalaInterpreterServlet extends ServletRegister {
  override protected def getEndpoints(): Array[String] = Array("/3/scalaint", "/3/scalaint/*", "/3/scalaint/result/*")

  override protected def getServlet(): Servlet = new ScalaInterpreterServlet
}
