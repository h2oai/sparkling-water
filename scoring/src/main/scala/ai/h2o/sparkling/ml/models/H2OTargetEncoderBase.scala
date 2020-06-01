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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OTargetEncoderMOJOParams
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

trait H2OTargetEncoderBase extends PipelineStage with H2OTargetEncoderMOJOParams {
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    StructType(schema.fields ++ getOutputCols().map(StructField(_, DoubleType, nullable = true)))
  }

  private def validateSchema(flatSchema: StructType): Unit = {
    require(getLabelCol() != null, "Label column can't be null!")
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")
    require(getOutputCols() != null && getOutputCols().nonEmpty, "The list of output columns can't be null or empty!")
    require(
      getInputCols().length == getOutputCols().length,
      "The number of input columns and output columns should be equal!")
    val fields = flatSchema.fields
    val fieldNames = fields.map(_.name)
    require(
      fieldNames.contains(getLabelCol()),
      s"The specified label column '${getLabelCol()}' was not found in the input dataset!")
    getInputCols().foreach { inputCol =>
      require(
        fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    val inputAndOutputIntersection = getInputCols().intersect(getOutputCols())
    require(
      inputAndOutputIntersection.isEmpty,
      s"""The columns [${inputAndOutputIntersection.map(i => s"'$i'").mkString(", ")}] are specified
         |as input columns and also as output columns. There can't be an overlap.""".stripMargin)
    val schemaAndOutputIntersection = fieldNames.intersect(getOutputCols())
    require(
      schemaAndOutputIntersection.isEmpty,
      s"""The output columns [[${schemaAndOutputIntersection.map(i => s"'$i'").mkString(", ")}]] are present already
         |in the input dataset.""".stripMargin)
  }
}
