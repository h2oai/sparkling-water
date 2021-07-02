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

import ai.h2o.sparkling.ml.models.H2OFeatureEstimatorBase
import org.apache.spark.sql.types.{StructField, StructType}

trait H2ODimReductionExtraParams extends H2OFeatureEstimatorBase with HasOutputCol with HasInputColsOnMOJO {

  protected override def outputSchema: Seq[StructField] = {
    val outputType = org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    val outputField = StructField(getOutputCol(), outputType, nullable = false)
    Seq(outputField)
  }

  protected override def validate(schema: StructType): Unit = {
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")
    require(getOutputCol() != null, "The output column can't be null!")
    val fieldNames = schema.fieldNames
    getInputCols().foreach { inputCol =>
      require(
        fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    require(
      !fieldNames.contains(getOutputCol()),
      s"The output column '${getOutputCol()}' is already present in the dataset!")
  }

  protected def copyExtraParams(to: H2ODimReductionExtraParams): Unit = {
    to.set(to.inputCols -> getInputCols())
    to.setOutputCol(getOutputCol())
  }
}
