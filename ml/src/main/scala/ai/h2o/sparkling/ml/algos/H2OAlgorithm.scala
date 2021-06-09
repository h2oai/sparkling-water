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

import ai.h2o.sparkling.ml.models.{H2OAlgorithmMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OAlgorithmCommonParams
import hex.Model
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[P <: Model.Parameters: ClassTag]
  extends H2OEstimator[P]
  with H2OAlgorithmCommonParams {

  override private[sparkling] def getInputCols(): Array[String] = getFeaturesCols()

  override private[sparkling] def setInputCols(value: Array[String]): this.type = setFeaturesCols(value)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def fit(dataset: Dataset[_]): H2OAlgorithmMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OAlgorithmMOJOModel]
  }

  override protected def createMOJOSettings(): H2OMOJOSettings = H2OMOJOSettings.createFromModelParams(this)
}
