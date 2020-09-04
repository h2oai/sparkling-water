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

import ai.h2o.sparkling.ml.models.H2OBinaryModel
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col

trait H2OAlgoCommonUtils extends EstimatorCommonUtils {

  protected var binaryModel: Option[H2OBinaryModel] = None

  def getBinaryModel(): H2OBinaryModel = {
    if (binaryModel.isEmpty) {
      throw new IllegalArgumentException("Algorithm needs to be fit first in order to access binary model features.")
    }
    binaryModel.get
  }

  private[sparkling] def getExcludedCols(): Seq[String] = Seq.empty

  /** The list of additional columns that needs to be send to H2O-3 backend for model training. */
  private[sparkling] def getAdditionalCols(): Seq[String] = Seq.empty

  /** The list of additional columns that needs to be send to H2O-3 backend for model validation. */
  private[sparkling] def getAdditionalValidationCols(): Seq[String] = Seq.empty

  private[sparkling] def getFeaturesCols(): Array[String]

  private[sparkling] def getColumnsToCategorical(): Array[String]

  private[sparkling] def getSplitRatio(): Double

  private[sparkling] def setFeaturesCols(value: Array[String]): this.type

  private[sparkling] def getValidationDataFrame(): DataFrame

  private[sparkling] def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame]) = {
    val excludedCols = getExcludedCols()

    if (getFeaturesCols().isEmpty) {
      val features = dataset.columns.filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
      setFeaturesCols(features)
    } else {
      val missingColumns = getFeaturesCols()
        .filterNot(col => dataset.columns.contains(col))

      if (missingColumns.nonEmpty) {
        throw new IllegalArgumentException(
          "The following feature columns are not available on" +
            s" the training dataset: '${missingColumns.mkString(", ")}'")
      }
    }

    val featureColumns = getFeaturesCols().map(sanitize).map(col)

    if (dataset.select(featureColumns: _*).distinct().count() == 1) {
      throw new IllegalArgumentException(s"H2O could not use any of the specified features" +
        s" columns: '${getFeaturesCols().mkString(", ")}' because they are all constants. H2O requires at least one non-constant column.")
    }
    val excludedColumns = excludedCols.map(sanitize).map(col)
    val additionalColumns = getAdditionalCols().map(sanitize).map(col)
    val columns = (featureColumns ++ excludedColumns ++ additionalColumns).distinct
    val h2oContext = H2OContext.ensure(
      "H2OContext needs to be created in order to train the model. Please create one as H2OContext.getOrCreate().")
    val trainFrame = h2oContext.asH2OFrame(dataset.select(columns: _*).toDF())

    // Our MOJO wrapper needs the full column name before the array/vector expansion in order to do predictions
    trainFrame.convertColumnsToCategorical(getColumnsToCategorical())

    val validationDataFrame = getValidationDataFrame()
    if (validationDataFrame != null) {
      val additionalValidationColumns = getAdditionalValidationCols().map(sanitize).map(col)
      val validationColumns = (columns ++ additionalValidationColumns).distinct
      val validationFrame = h2oContext.asH2OFrame(validationDataFrame.select(validationColumns: _*))
      (trainFrame, Some(validationFrame))
    } else if (getSplitRatio() < 1.0) {
      val frames = trainFrame.split(getSplitRatio())
      if (frames.length > 1) {
        (frames(0), Some(frames(1)))
      } else {
        (frames(0), None)
      }
    } else {
      (trainFrame, None)
    }
  }

  def sanitize(colName: String) = '`' + colName + '`'
}
