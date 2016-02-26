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
package water.api.scalaInt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.H2OConf
import org.apache.spark.repl.h2o.H2OInterpreter
import water.Iced
import water.api.Handler
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap
import scala.compat.Platform

/**
 * Handler for all Scala related endpoints
 */
class ScalaCodeHandler(val sc: SparkContext) extends Handler with H2OConf{

  val intrPoolSize = scalaIntDefaultNum
  val freeInterpreters = new java.util.concurrent.ConcurrentLinkedQueue[H2OInterpreter]
  val timeout = scalaIntTimeout
  var mapIntr = new TrieMap[Int, (H2OInterpreter, Long)]
  var lastIdUsed = 0
  initializeHandler()

  def interpret(version: Int, s: ScalaCodeV3): ScalaCodeV3 = {
    // check if session exists
    if (s.session_id == -1 || !mapIntr.isDefinedAt(s.session_id)) {
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }
    mapIntr += s.session_id ->(mapIntr(s.session_id)._1, Platform.currentTime) // update the time
    val intp = mapIntr(s.session_id)._1
    s.status = intp.runCode(s.code).toString
    s.response = intp.interpreterResponse
    s.output = intp.consoleOutput
    s
  }

  def initSession(version: Int, s: ScalaSessionIdV3): ScalaSessionIdV3 = {
    val intp = fetchInterpreter()
    s.session_id = intp.sessionId
    s
  }

  def fetchInterpreter(): H2OInterpreter = {
    this.synchronized {
                        if (!freeInterpreters.isEmpty) {
                          val intp = freeInterpreters.poll()
                          mapIntr.put(intp.sessionId, (intp, Platform.currentTime))
                          new Thread(new Runnable {
                            def run(): Unit = {
                              createIntpInPool()
                            }
                          }).start()
                          intp
                        } else {
                          // pool is empty at the moment and is being filled, return new interpreter without using the pool
                          val id = createID()
                          val intp = new H2OInterpreter(sc, id)
                          mapIntr.put(intp.sessionId, (intp, Platform.currentTime))
                          intp
                        }
                      }
  }

  def destroySession(version: Int, s: ScalaSessionIdV3): ScalaSessionIdV3 = {
    if(!mapIntr.contains(s.session_id)){
      throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
    }
    mapIntr(s.session_id)._1.closeInterpreter()
    mapIntr -= s.session_id
    s
  }

  def getSessions(version: Int, s: ScalaSessionsV3): ScalaSessionsV3 = {
    s.sessions = mapIntr.keys.toArray
    s
  }

  def initializeHandler(): Unit = {
    initializePool()
    val checkThread = new Thread(new Runnable {
      def run() {
        while (true) {
          mapIntr.foreach { case (id: Int, (intr: H2OInterpreter, lastChecked: Long)) =>
            if (Platform.currentTime - lastChecked >= timeout) {
              mapIntr(id)._1.closeInterpreter()
              mapIntr -= id
            }
                          }
          Thread.sleep(timeout)
        }
      }
    })
    checkThread.start()
  }

  def initializePool(): Unit = {
    for (i <- 0 until intrPoolSize) {
      createIntpInPool()
    }
  }

  def createIntpInPool(): H2OInterpreter = {
    val id = createID()
    val intp = new H2OInterpreter(sc, id)
    freeInterpreters.add(intp)
    intp
  }

  def createID(): Int = {
    this.synchronized {
                        lastIdUsed = lastIdUsed + 1
                        lastIdUsed
                      }
  }

  /* Required Spark config */
  override def sparkConf: SparkConf = sc.getConf
}

private[api] class IcedCode(val session_id: Int, val code: String) extends Iced[IcedCode] {

  def this() = this(-1, null)
  // initialize with dummy values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}

private[api] class IcedSessions extends Iced[IcedSessions] {}

private[api] class IcedSessionId(val rdd_id: Integer) extends Iced[IcedSessionId] {

  def this() = this(-1) // initialize with empty values, this is used by the createImpl method in the
  //RequestServer, as it calls constructor without any arguments
}

