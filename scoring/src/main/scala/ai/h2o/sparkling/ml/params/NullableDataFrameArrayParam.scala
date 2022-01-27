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

import ai.h2o.sparkling.utils.{DataFrameJsonSerialization, DataFrameSerializer}
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class NullableDataFrameArrayParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: Array[DataFrame] => Boolean)
  extends Param[Array[DataFrame]](parent, name, doc, isValid) {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[DataFrame]): ParamPair[Array[DataFrame]] =
    w(value.asScala.toArray)

  override def jsonEncode(dataFrames: Array[DataFrame]): String = {
    val serializerClassName = parent.getDataFrameSerializer()
    val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
    DataFrameJsonSerialization.encodeDataFrames(dataFrames, serializer)
  }

  override def jsonDecode(json: String): Array[DataFrame] = DataFrameJsonSerialization.decodeDataFrames(json)
}
