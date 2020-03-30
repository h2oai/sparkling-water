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

import ai.h2o.sparkling.repl.H2OInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import water.H2O.H2OCountedCompleter
import water._
import water.api._
import water.api.schemas3.JobV3
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap

/**
  * Handler for all Scala related endpoints
  */
class ScalaCodeHandler(val sc: SparkContext, val h2oContext: H2OContext) extends Handler {

  val intrPoolSize = h2oContext.getConf.scalaIntDefaultNum
  val freeInterpreters = new java.util.concurrent.ConcurrentLinkedQueue[H2OInterpreter]
  var mapIntr = new TrieMap[Int, H2OInterpreter]
  val lastIdUsed = new AtomicInteger(0)
  val jobCount = new AtomicInteger(0)
  initializeInterpreterPool()

  def interpret(version: Int, s: ScalaCodeV3): ScalaCodeV3 = {
    // check if session exists
    if (s.session_id == -1 || !mapIntr.isDefinedAt(s.session_id)) {
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }

    val job = new Job[ScalaCodeResult](Key.make[ScalaCodeResult](), classOf[ScalaCodeResult].getName, "ScalaCodeResult")

    this.synchronized {
      jobCount.incrementAndGet()
      while (h2oContext.getConf.maxParallelScalaCellJobs != -1 && jobCount
               .intValue() > h2oContext.getConf.maxParallelScalaCellJobs) {
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
    if (!h2oContext.getConf.flowScalaCellAsync) {
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
    s.async = h2oContext.getConf.flowScalaCellAsync
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

  def fetchInterpreter(): H2OInterpreter = {
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
        val intp = new H2OInterpreter(sc, id)
        mapIntr.put(intp.sessionId, intp)
        intp
      }
    }
  }

  def destroySession(version: Int, s: ScalaSessionIdV3): ScalaSessionIdV3 = {
    if (!mapIntr.contains(s.session_id)) {
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }
    mapIntr(s.session_id).closeInterpreter()
    mapIntr -= s.session_id
    s
  }

  def getSessions(version: Int, s: ScalaSessionsV3): ScalaSessionsV3 = {
    s.sessions = mapIntr.keys.toArray
    s
  }

  def initializeInterpreterPool(): Unit = {
    for (_ <- 0 until intrPoolSize) {
      createInterpreterInPool()
    }
  }

  def createInterpreterInPool(): H2OInterpreter = {
    val id = lastIdUsed.incrementAndGet()
    val intp = new H2OInterpreter(sc, id)
    freeInterpreters.add(intp)
    intp
  }

}

object ScalaCodeHandler {

  private[api] def registerEndpoints(context: RestApiContext, sc: SparkContext, h2oContext: H2OContext) = {
    val scalaCodeHandler = new ScalaCodeHandler(sc, h2oContext)

    def scalaCodeFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = scalaCodeHandler
    }

    context.registerEndpoint(
      "interpretScalaCode",
      "POST",
      "/3/scalaint/{session_id}",
      classOf[ScalaCodeHandler],
      "interpret",
      "Interpret the code and return the result",
      scalaCodeFactory)

    context.registerEndpoint(
      "initScalaSession",
      "POST",
      "/3/scalaint",
      classOf[ScalaCodeHandler],
      "initSession",
      "Return session id for communication with scala interpreter",
      scalaCodeFactory)

    context.registerEndpoint(
      "getScalaSessions",
      "GET",
      "/3/scalaint",
      classOf[ScalaCodeHandler],
      "getSessions",
      "Return all active session IDs",
      scalaCodeFactory)

    context.registerEndpoint(
      "destroyScalaSession",
      "DELETE",
      "/3/scalaint/{session_id}",
      classOf[ScalaCodeHandler],
      "destroySession",
      "Return session id for communication with scala interpreter",
      scalaCodeFactory)

    context.registerEndpoint(
      "getScalaCodeResult",
      "POST",
      "/3/scalaint/result/{result_key}",
      classOf[ScalaCodeHandler],
      "getScalaCodeResult",
      "Get Scala code result for desired job",
      scalaCodeFactory)
  }
}
