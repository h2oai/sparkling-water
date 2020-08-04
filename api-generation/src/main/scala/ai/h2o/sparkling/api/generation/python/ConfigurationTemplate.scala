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

package ai.h2o.sparkling.api.generation.python

import java.lang.reflect.Method

object ConfigurationTemplate extends ((Array[Method], Array[Method], Class[_]) => String) {

  def apply(getters: Array[Method], setters: Array[Method], entity: Class[_]): String = {

    s"""#
       |# Licensed to the Apache Software Foundation (ASF) under one or more
       |# contributor license agreements.  See the NOTICE file distributed with
       |# this work for additional information regarding copyright ownership.
       |# The ASF licenses this file to You under the Apache License, Version 2.0
       |# (the "License"); you may not use this file except in compliance with
       |# the License.  You may obtain a copy of the License at
       |#
       |#    http://www.apache.org/licenses/LICENSE-2.0
       |#
       |# Unless required by applicable law or agreed to in writing, software
       |# distributed under the License is distributed on an "AS IS" BASIS,
       |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |# See the License for the specific language governing permissions and
       |# limitations under the License.
       |#
       |
       |import warnings
       |from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils
       |
       |
       |class ${entity.getSimpleName}(SharedBackendConfUtils):
       |
       |    #
       |    # Getters
       |    #
       |
       |${generateGetters(getters)}
       |
       |    #
       |    # Setters
       |    #
       |
       |${generateSetters(setters)}""".stripMargin
  }

  private def generateGetters(getters: Array[Method]): String = {
    getters.map(generateGetter).mkString("\n")
  }

  private def generateSetters(setters: Array[Method]): String = {
    setters.map(generateSetter).mkString("\n")
  }

  private def generateGetter(m: Method): String = {
    s"""    def ${m.getName}(self):
       |        ${getReturnLine(m)}
       |""".stripMargin
  }

  private def generateSetter(m: Method): String = {
    if (m.getParameterCount == 0) {
      s"""    def ${m.getName}(self):
         |        self._jconf.${m.getName}()
         |        return self
         |""".stripMargin
    } else {
      s"""    def ${m.getName}(self, value):
         |        self._jconf.${m.getName}(value)
         |        return self
         |""".stripMargin
    }
  }

  private def getReturnLine(m: Method): String = {
    m.getReturnType match {
      case clz if clz.getName == "scala.Option" => s"return self._get_option(self._jconf.${m.getName}())"
      case _ => s"return self._jconf.${m.getName}()"
    }
  }
}
