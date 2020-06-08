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

import ai.h2o.sparkling.api.generation.common.EntitySubstitutionContext

trait PythonEntityTemplate {
  protected def generateEntity(properties: EntitySubstitutionContext)(content: String): String = {
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
       |${generateImports(properties)}
       |
       |
       |class ${properties.entityName}${referencesToInheritedClasses(properties)}:
       |
       |$content
       |""".stripMargin
  }

  private def generateImports(substitutionContext: EntitySubstitutionContext): String = {
    substitutionContext.imports
      .map { i =>
        val parts = i.split('.')
        val namespace = parts.take(parts.length - 1).mkString(".")
        val clazz = parts.last
        s"from $namespace import $clazz"
      }
      .mkString("\n")
  }

  private def referencesToInheritedClasses(substitutionContext: EntitySubstitutionContext): String = {
    if (substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      substitutionContext.inheritedEntities.mkString("(", ", ", ")")
    }
  }
}
