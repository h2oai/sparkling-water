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

package ai.h2o.sparkling.ml.params

import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods.{compact, render}

class NonSerializableNullableDataFrameParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: DataFrame => Boolean)
  extends Param[DataFrame](parent, name, doc + " The parameter is not serializable!", isValid)
  with Logging {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, (_: DataFrame) => true)

  override def jsonEncode(dataFrame: DataFrame): String = {
    if (dataFrame != null) {
      logWarning(
        s"The parameter '$name' of the data frame type has been set, " +
          "but the value won't be serialized since the data frame can be potentially really big.")
    }
    compact(render(JNull))
  }

  override def jsonDecode(json: String): DataFrame = null
}
