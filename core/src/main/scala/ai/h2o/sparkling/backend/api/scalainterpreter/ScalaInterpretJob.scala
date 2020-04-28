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

import java.util.concurrent.{Callable, ExecutorService}

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.repl.H2OInterpreter
import water.api.schemas3.JobV3

import scala.collection.concurrent.TrieMap

/**
  * In Asynchronous mode of Scala cell execution H2O Flow expects that the cell returns H2O Job right away and then
  * when clicking on the job link, the user will either get informed that the job is still running or the result will
  * be displayed.
  *
  * We need to perform the computation on Spark Driver to have access to both H2O and Spark and since H2O expects the H2O
  * Job object, we are submitting dummy H2O job which is alive for the duration of the computation on the Spark side.
  *
  * That is the reason for submitDummyWrapperH2OJob and stopDummyWrapperH2OJob methods.
  */
private[api] class ScalaInterpretJob(
    val intp: H2OInterpreter,
    conf: H2OConf,
    code: String,
    jobResults: TrieMap[String, ScalaCodeResult],
    threadPool: ExecutorService)
  extends RestCommunication {

  def run(): ScalaCode = {
    val resultKey = s"${intp.sessionId}_${System.currentTimeMillis()}"
    if (conf.flowScalaCellAsync) {
      val job = submitDummyWrapperH2OJob(resultKey)
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          val result = ScalaCodeResult(code, intp.runCode(code).toString, intp.interpreterResponse, intp.consoleOutput)
          jobResults.put(resultKey, result)
          stopDummyWrapperH2OJob(job.key.name)
        }
      })
      ScalaCode(intp.sessionId, code, resultKey, null, null, null, job)
    } else {
      val future = threadPool.submit(new Callable[ScalaCode] {
        override def call(): ScalaCode = {
          val result = ScalaCodeResult(code, intp.runCode(code).toString, intp.interpreterResponse, intp.consoleOutput)
          jobResults.put(resultKey, result)
          ScalaCode(intp.sessionId, code, resultKey, result.scalaStatus, result.scalaResponse, result.scalaOutput, null)
        }
      })
      future.get()
    }
  }

  /**
    * This method calls internal Sparkling Water endpoint which starts the given dummy job.
    */
  private def submitDummyWrapperH2OJob(resultKey: String): JobV3 = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    update[JobV3](endpoint, s"/3/SparklingInternal/start/$resultKey", conf)
  }

  /**
    * This method calls internal Sparkling Water endpoint which stops the given dummy job.
    */
  private def stopDummyWrapperH2OJob(jobId: String): Unit = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    readURLContent(
      endpoint,
      "POST",
      s"/3/SparklingInternal/stop/$jobId",
      conf,
      Map.empty,
      encodeParamsAsJson = false,
      None)
  }
}
