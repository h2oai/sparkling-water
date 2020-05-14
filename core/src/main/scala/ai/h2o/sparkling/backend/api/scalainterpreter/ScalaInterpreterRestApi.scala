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

import java.net.URI

import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.h2o.H2OContext

trait ScalaInterpreterRestApi extends RestApiUtils {

  protected def initSession(): ScalaSessionId = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[ScalaSessionId](endpoint, "/3/scalaint", hc.getConf)
  }

  protected def destroySession(sessionId: Int): Unit = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    delete[ScalaSessionId](endpoint, s"/3/scalaint/$sessionId", hc.getConf)
  }

  protected def getSessions(): ScalaSessions = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[ScalaSessions](endpoint, "/3/scalaint", hc.getConf)
  }

  protected def interpret(sessionId: Int, code: String): ScalaCode = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = Map("code" -> code)
    update[ScalaCode](endpoint, s"/3/scalaint/$sessionId", hc.getConf, params)
  }

  protected def getScalaCodeResult(resultKey: String): ScalaCodeResult = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[ScalaCodeResult](endpoint, s"/3/scalaint/result/$resultKey", hc.getConf)
  }
}
