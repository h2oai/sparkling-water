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
import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

trait H2OAutoEncoderExtraParams extends H2OFeatureEstimatorBase with HasOutputCol with HasInputColsOnMOJO {

  private val originalCol: Param[String] = new Param[String](
    parent = this,
    name = "originalCol",
    doc = "Original column name. This column contains input values to the neural network of auto encoder.")

  private val withOriginalCol: Param[Boolean] = new BooleanParam(
    parent = this,
    name = "withOriginalCol",
    doc = "A flag identifying whether a column with input values to the neural network will be produced or not.")

  private val mseCol: Param[String] = new Param[String](
    parent = this,
    name = "mseCol",
    doc = "MSE column name. This column contains mean square error calculated from original and output values.")

  private val withMSECol: Param[Boolean] = new BooleanParam(
    parent = this,
    name = "withMSECol",
    doc = "A flag identifying whether a column with mean square error will be produced or not.")

  setDefault(
    originalCol -> (uid + "__original"),
    withOriginalCol -> false,
    mseCol -> (uid + "__mse"),
    withMSECol -> false)

  //
  // Getters
  //
  def getOriginalCol(): String = $(originalCol)

  def getWithOriginalCol(): Boolean = $(withOriginalCol)

  def getMSECol(): String = $(mseCol)

  def getWithMSECol(): Boolean = $(withMSECol)

  //
  // Setters
  //
  def setOriginalCol(name: String): this.type = set(originalCol -> name)

  def setWithOriginalCol(flag: Boolean): this.type = set(withOriginalCol -> flag)

  def setMSECol(name: String): this.type = set(mseCol -> name)

  def setWithMSECol(flag: Boolean): this.type = set(withMSECol -> flag)

  protected override def outputSchema: Seq[StructField] = {
    val outputType = org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    val nil = Nil

    val withReconstructionErrorField = if (getWithMSECol()) {
      val reconstructionErrorField = StructField(getMSECol(), DoubleType, nullable = false)
      reconstructionErrorField :: nil
    } else {
      nil
    }

    val withOriginalField = if (getWithOriginalCol()) {
      val originalField = StructField(getOriginalCol(), outputType, nullable = false)
      originalField :: withReconstructionErrorField
    } else {
      withReconstructionErrorField
    }

    val outputField = StructField(getOutputCol(), outputType, nullable = false)
    outputField :: withOriginalField
  }

  protected override def validate(schema: StructType): Unit = {
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")
    require(getOutputCol() != null, "The output column can't be null!")
    require(getOriginalCol() != null || !getWithOriginalCol(), "The original column can't be null!")
    require(getMSECol() != null || !getWithMSECol(), "The original column can't be null!")
    val fieldNames = schema.fieldNames
    getInputCols().foreach { inputCol =>
      require(
        fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    require(
      !fieldNames.contains(getOutputCol()),
      s"The output column '${getOutputCol()}' is already present in the dataset!")
    require(
      !fieldNames.contains(getOriginalCol()) || !getWithOriginalCol(),
      s"The original column '${getOriginalCol()}' is already present in the dataset!")
    require(
      !fieldNames.contains(getMSECol()) || !getWithMSECol(),
      s"The mean square error column '${getMSECol()}' is already present in the dataset!")
  }

  protected def copyExtraParams(to: H2OAutoEncoderExtraParams): Unit = {
    to.set(to.inputCols -> getInputCols())
    to.setOutputCol(getOutputCol())
    to.setOriginalCol(getOriginalCol())
    to.setWithOriginalCol(getWithOriginalCol())
    to.setMSECol(getMSECol())
    to.setWithMSECol(getWithMSECol())
  }
}
