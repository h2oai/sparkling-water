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
package org.apache.spark.ml.h2o.param

import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.{Estimator, PipelineStage}
import org.apache.spark.ml.h2o.models.H2OTargetEncoderModel
import org.apache.spark.ml.param.{Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

trait H2OTargetEncoderParams extends PipelineStage with Params {

  //
  // Override pipeline stage members
  //
  override def transformSchema(schema: StructType): StructType = {
    val flatSchema = H2OSchemaUtils.flattenSchema(schema)
    require(getLabelCol() != null, "Label column can't be null!")
    require(getInputCols() != null && getLabelCol().isEmpty, "The list of input columns can't be null or empty!")
    val fields = flatSchema.fields
    require(fields.contains(getLabelCol()),
      s"The specified label column '${getLabelCol()}' was not found in the input dataset!")
    for(inputCol <- getInputCols()) {
      require(fields.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    StructType(flatSchema.fields ++ getOutputCols().map(StructField(_, DoubleType, nullable = false)))
  }

  //
  // List of Parameters
  //
  private val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")
  private val labelCol = new Param[String](this, "labelCol", "Label column name")
  private val inputCols = new StringArrayParam(this, "inputCols", "Names of columns that will be transformed")

  //
  // Default values
  //
  setDefault(
    foldCol -> null,
    labelCol -> "label",
    inputCols -> Array[String]()
  )

  //
  // Getters
  //
  def getFoldCol(): String = $(foldCol)

  def getLabelCol(): String = $(labelCol)

  def getInputCols(): Array[String] = $(inputCols)

  def getOutputCols(): Array[String] = getInputCols().map(_ + "_te")

  //
  // Setter
  //
  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)
}
