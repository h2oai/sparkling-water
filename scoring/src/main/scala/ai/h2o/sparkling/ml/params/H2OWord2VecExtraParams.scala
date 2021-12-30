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
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, StringType, StructField, StructType}

trait H2OWord2VecExtraParams extends H2OFeatureEstimatorBase with HasOutputCol with HasInputColOnMOJO {

  override protected def validate(schema: StructType): Unit = {
    val inputColumn = getInputCol()
    val outputColumn = getOutputCol()
    require(inputColumn != null, "The input column can't be null!")
    require(outputColumn != null, "The output column can't be null!")

    val fields = schema.fields
    fields.find(_.name == inputColumn) match {
      case Some(inputColField) if !isStringArray(inputColField.dataType) =>
        throw new IllegalArgumentException(
          s"The specified input column '$inputColumn' type ('${inputColField.dataType}') is not an array of strings!")
      case None =>
        throw new IllegalArgumentException(
          s"The specified input column '$inputColumn' was not found in the input dataset!")
      case _ =>
    }
    require(
      inputColumn != outputColumn,
      s"""Input column is same as the output column. There can't be an overlap.""".stripMargin)
    require(
      !fields.map(_.name).contains(outputColumn),
      s"The output column $outputColumn is present already in the input dataset.")
  }

  override protected def outputSchema: Seq[StructField] = {
    val outputType = ArrayType(FloatType, containsNull = false)
    val outputField = StructField(getOutputCol(), outputType, nullable = true)
    Seq(outputField)
  }

  protected def copyExtraParams(to: H2OWord2VecExtraParams): Unit = {
    to.setInputCol(getInputCol())
    to.setOutputCol(getOutputCol())
  }

  private def isStringArray(dataType: DataType) = dataType match {
    case ArrayType(StringType, _) => true
    case _ => false
  }

}
