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

import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.ml.params.DataFrameSerializationWrapper._

import scala.collection.JavaConverters._

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OCommonParams extends H2OBaseMOJOParams {

  protected final val validationDataFrame = new NonSerializableNullableDataFrameParam(
    this,
    "validationDataFrame",
    "A data frame dedicated for a validation of the trained model. If the parameters is not set," +
      "a validation frame created via the 'splitRatio' parameter.")

  protected final val splitRatio = new DoubleParam(
    this,
    "splitRatio",
    "Accepts values in range [0, 1.0] which determine how large part of dataset is used for training and for validation. " +
      "For example, 0.8 -> 80% training 20% validation. This parameter is ignored when validationDataFrame is set.")

  protected final val columnsToCategorical =
    new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categorical before modelling")

  protected final val keepBinaryModels = new BooleanParam(
    this,
    "keepBinaryModels",
    "If set to true, all binary models created during execution of the ``fit`` method will be kept in DKV of H2O-3 cluster.")

  //
  // Default values
  //
  setDefault(
    validationDataFrame -> null,
    splitRatio -> 1.0, // Use whole frame as training frame
    columnsToCategorical -> Array.empty[String],
    keepBinaryModels -> false)

  //
  // Getters
  //
  def getValidationDataFrame(): DataFrame = $(validationDataFrame)

  def getSplitRatio(): Double = $(splitRatio)

  def getColumnsToCategorical(): Array[String] = $(columnsToCategorical)

  def getKeepBinaryModels(): Boolean = $(keepBinaryModels)

  //
  // Setters
  //
  def setValidationDataFrame(dataFrame: DataFrame): this.type = set(validationDataFrame, toWrapper(dataFrame))

  def setSplitRatio(ratio: Double): this.type = set(splitRatio, ratio)

  def setColumnsToCategorical(first: String, others: String*): this.type =
    set(columnsToCategorical, Array(first) ++ others)

  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)

  def setColumnsToCategorical(columnNames: java.util.ArrayList[String]): this.type = {
    setColumnsToCategorical(columnNames.asScala.toArray)
  }

  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type =
    set(convertUnknownCategoricalLevelsToNa, value)

  def setConvertInvalidNumbersToNa(value: Boolean): this.type = set(convertInvalidNumbersToNa, value)

  def setKeepBinaryModels(value: Boolean): this.type = set(keepBinaryModels, value)
}
