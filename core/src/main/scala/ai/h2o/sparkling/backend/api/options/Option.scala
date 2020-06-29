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
package ai.h2o.sparkling.backend.api.options

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.H2OConf
import water.exceptions.H2ONotFoundArgumentException

/** Schema representing /3/option endpoint */
case class Option(name: String, value: String)

object Option extends ParameterBase {
  private[options] case class OptionParameters(name: String) {
    def validate(conf: H2OConf): Unit = {
      if (!conf.contains(name)) {
        throw new H2ONotFoundArgumentException(s"The option '$name' is not specified on the H2OConf!")
      }
    }
  }

  private[options] object OptionParameters {
    def parse(request: HttpServletRequest): OptionParameters = {
      val name = getParameterAsString(request, "name")
      OptionParameters(name)
    }
  }
}
