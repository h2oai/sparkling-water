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

package ai.h2o.sparkling.api.generation.common

case class ParameterSubstitutionContext(
    namespace: String,
    entityName: String,
    h2oSchemaClass: Class[_],
    h2oParameterClass: Class[_],
    ignoredParameters: Seq[String],
    explicitFields: Seq[ExplicitField],
    deprecatedFields: Seq[DeprecatedField],
    explicitDefaultValues: Map[String, Any],
    typeExceptions: Map[String, Class[_]],
    defaultValueFieldPrefix: String = "_",
    defaultValueSource: DefaultValueSource.DefaultValueSource,
    defaultValuesOfCommonParameters: Map[String, Any],
    generateParamTag: Boolean)
  extends SubstitutionContextBase

case class ExplicitField(
    h2oName: String,
    implementation: String,
    defaultValue: Any,
    sparkName: Option[String] = None,
    mojoImplementation: Option[String] = None)

case class DeprecatedField(h2oName: String, implementation: String, sparkName: String, version: String)

object DefaultValueSource extends Enumeration {
  type DefaultValueSource = Value
  val Field, Getter = Value
}
