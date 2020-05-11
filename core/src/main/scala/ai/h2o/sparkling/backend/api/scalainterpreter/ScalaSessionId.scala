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

import ai.h2o.sparkling.backend.api.ParameterBase
import ai.h2o.sparkling.repl.H2OInterpreter
import javax.servlet.http.HttpServletRequest
import water.exceptions.H2ONotFoundArgumentException

import scala.collection.concurrent.TrieMap

/** Schema representing [DELETE] /3/scalaint endpoint */
case class ScalaSessionId(session_id: Int, async: Boolean)

object ScalaSessionId extends ParameterBase {

  def apply(sessionId: Int): ScalaSessionId = ScalaSessionId(sessionId, async = false)

  private[scalainterpreter] case class ScalaSessionIdParameters(sessionId: Int) {
    def validate(mapIntr: TrieMap[Int, H2OInterpreter]): Unit = {
      if (!mapIntr.contains(sessionId)) {
        throw new H2ONotFoundArgumentException("Session does not exists. Create session using the address /3/scalaint!")
      }
    }
  }

  object ScalaSessionIdParameters {
    private[scalainterpreter] def parse(request: HttpServletRequest): ScalaSessionIdParameters = {
      val sessionId = request.getRequestURI.split("/")(3).toInt
      ScalaSessionIdParameters(sessionId)
    }
  }
}
