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
package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OSupervisedMOJOModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import hex.Model

import scala.reflect.ClassTag

abstract class H2OSupervisedAlgorithmWithFoldColumn[P <: Model.Parameters: ClassTag] extends H2OSupervisedAlgorithm[P] {

  def getFoldCol(): String

  def setFoldCol(value: String): this.type

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    require(
      getOffsetCol() == null || getOffsetCol() != getFoldCol(),
      "Specified offset column cannot be the same as the fold column!")
    require(
      getWeightCol() == null || getWeightCol() != getFoldCol(),
      "Specified weight column cannot be the same as the fold column!")
    transformedSchema
  }

  override def fit(dataset: Dataset[_]): H2OSupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OSupervisedMOJOModel]
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
