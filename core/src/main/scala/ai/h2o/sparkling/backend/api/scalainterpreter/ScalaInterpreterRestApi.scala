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

import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.h2o.H2OContext

trait ScalaInterpreterRestApi extends RestApiUtils {

  protected def initSession(): ScalaSessionIdV3 = {
    val conf = H2OContext.ensure().getConf
    val endpoint = getClusterEndpoint(conf)
    update[ScalaSessionIdV3](endpoint, "/3/scalaint", conf)
  }

  protected def destroySession(sessionId: Int): Unit = {
    val conf = H2OContext.ensure().getConf
    val endpoint = getClusterEndpoint(conf)
    //delete[ScalaSessionIdV3](endpoint, s"/3/scalaint/$sessionId", conf)
  }

  protected def getSessions(): ScalaSessions = {
    val conf = H2OContext.ensure().getConf
    val endpoint = getClusterEndpoint(conf)
    query[ScalaSessions](endpoint, "/3/scalaint", conf)
  }

  protected def interpret(code: String, sessionId: Int): ScalaCodeV3 = {
    val conf = H2OContext.ensure().getConf
    val endpoint = getClusterEndpoint(conf)
    val params = Map("code" -> code)
    update[ScalaCodeV3](endpoint, s"/3/scalaint/$sessionId", conf, params)
  }
}
