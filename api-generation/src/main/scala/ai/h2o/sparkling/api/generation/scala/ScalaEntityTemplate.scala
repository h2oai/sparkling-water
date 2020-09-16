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

package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common.EntitySubstitutionContext

trait ScalaEntityTemplate {
  protected def generateEntity(properties: EntitySubstitutionContext, entityType: String)(content: String): String = {
    s"""/*
       | * Licensed to the Apache Software Foundation (ASF) under one or more
       | * contributor license agreements.  See the NOTICE file distributed with
       | * this work for additional information regarding copyright ownership.
       | * The ASF licenses this file to You under the Apache License, Version 2.0
       | * (the "License"); you may not use this file except in compliance with
       | * the License.  You may obtain a copy of the License at
       | *
       | *    http://www.apache.org/licenses/LICENSE-2.0
       | *
       | * Unless required by applicable law or agreed to in writing, software
       | * distributed under the License is distributed on an "AS IS" BASIS,
       | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       | * See the License for the specific language governing permissions and
       | * limitations under the License.
       | */
       |
       |package ${properties.namespace}
       |
       |${generateImports(properties)}
       |
       |$entityType ${properties.entityName}${properties.parameters}${referencesToInheritedClasses(properties)} {
       |
       |$content
       |}
    """.stripMargin
  }

  private def generateImports(substitutionContext: EntitySubstitutionContext): String = {
    substitutionContext.imports.map(i => s"import $i").mkString("\n")
  }

  private def referencesToInheritedClasses(substitutionContext: EntitySubstitutionContext): String = {
    if (substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      val head = substitutionContext.inheritedEntities.head
      val tail = substitutionContext.inheritedEntities.tail
      val headResult = s"\n  extends $head"
      val result = Seq(headResult) ++ tail.map(entity => s"\n  with $entity")
      result.mkString("")
    }
  }

  protected def stringify(value: Any): String = value match {
    case f: java.lang.Float => s"${f.toString.toLowerCase}f"
    case d: java.lang.Double => d.toString.toLowerCase
    case l: java.lang.Long => s"${l}L"
    case a: Array[_] => s"Array(${a.map(stringify).mkString(", ")})"
    case s: String => s""""$s""""
    case v if v == null => null
    case v => v.toString
  }
}
