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

import ai.h2o.sparkling.api.generation.common.CommonSubstitutionContext

trait ScalaEntityTemplate {
  def generateEntity(substitutionContext: CommonSubstitutionContext, entityType: String)(content: String): String = {
    s"""
       |/*
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
       |package ${substitutionContext.namespace}
       |${generateImports(substitutionContext)}
       |
       |$entityType ${substitutionContext.entityName}
       |${referencesToInheritedClasses(substitutionContext)} {
       |
       |$content
       |}
    """.stripMargin
  }

  private def generateImports(substitutionContext: CommonSubstitutionContext): String = {
    substitutionContext.imports.map(i => s"\n  import $i").mkString
  }

  private def referencesToInheritedClasses(substitutionContext: CommonSubstitutionContext): String = {
    if(substitutionContext.inheritedEntities.isEmpty) {
      ""
    } else {
      val head = substitutionContext.inheritedEntities.head
      val tail = substitutionContext.inheritedEntities.tail
      val headResult = s"  extends $head"
      val result = Seq(headResult) ++ tail.map(entity => s"  with $entity")
      result.mkString("\n")
    }
  }
}
