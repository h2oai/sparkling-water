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

import ai.h2o.sparkling.macros.DeprecatedMethod
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OCommonParams extends H2OBaseMOJOParams with H2OAlgoParamsBase {

  protected final val validationDataFrame = new NullableDataFrameParam(
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

  //
  // Default values
  //
  setDefault(
    validationDataFrame -> null,
    splitRatio -> 1.0, // Use whole frame as training frame
    columnsToCategorical -> Array.empty[String])

  //
  // Getters
  //
  override def getFeaturesCols(): Array[String] = {
    val excludedCols = getExcludedCols()
    $(featuresCols).filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
  }

  def getValidationDataFrame(): DataFrame = $(validationDataFrame)

  def getSplitRatio(): Double = $(splitRatio)

  def getColumnsToCategorical(): Array[String] = $(columnsToCategorical)

  //
  // Setters
  //
  def setValidationDataFrame(dataFrame: DataFrame): this.type = set(validationDataFrame, dataFrame)

  def setSplitRatio(ratio: Double): this.type = set(splitRatio, ratio)

  def setColumnsToCategorical(first: String, others: String*): this.type =
    set(columnsToCategorical, Array(first) ++ others)

  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)

  def setColumnsToCategorical(columnNames: java.util.ArrayList[String]): this.type = {
    setColumnsToCategorical(columnNames.asScala.toArray)
  }

  // Setters for parameters which are defined on MOJO as well
  def setPredictionCol(columnName: String): this.type = set(predictionCol, columnName)

  def setDetailedPredictionCol(columnName: String): this.type = set(detailedPredictionCol, columnName)

  @DeprecatedMethod(version = "3.34")
  def setWithDetailedPredictionCol(enabled: Boolean): this.type = this

  def setWithContributions(enabled: Boolean): this.type = set(withContributions, enabled)

  def setWithLeafNodeAssignments(enabled: Boolean): this.type = set(withLeafNodeAssignments, enabled)

  def setWithStageResults(enabled: Boolean): this.type = set(withStageResults, enabled)

  def setWithReconstructedData(enabled: Boolean): this.type = set(withReconstructedData, enabled)

  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(columnNames: Array[String]): this.type = {
    require(columnNames.length > 0, "Array with feature columns must contain at least one column.")
    set(featuresCols, columnNames)
  }

  def setFeaturesCols(columnNames: java.util.ArrayList[String]): this.type = {
    setFeaturesCols(columnNames.asScala.toArray)
  }

  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type =
    set(convertUnknownCategoricalLevelsToNa, value)

  def setConvertInvalidNumbersToNa(value: Boolean): this.type = set(convertInvalidNumbersToNa, value)

  def setNamedMojoOutputColumns(value: Boolean): this.type = set(namedMojoOutputColumns, value)

  private[sparkling] def getExcludedCols(): Seq[String]
}
