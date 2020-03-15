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

import ai.h2o.sparkling.frame.H2OFrame
import ai.h2o.sparkling.ml.params.H2OCommonParams
import ai.h2o.sparkling.ml.utils.{EstimatorCommonUtils, SchemaUtils}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait H2OAlgoCommonUtils extends H2OCommonParams with EstimatorCommonUtils {

  protected def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame], Array[String]) = {
    val excludedCols = getExcludedCols()

    if ($(featuresCols).isEmpty) {
      val features = dataset.columns.filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
      setFeaturesCols(features)
    } else {
      val missingColumns = getFeaturesCols()
        .filterNot(col => dataset.columns.contains(col))

      if (missingColumns.nonEmpty) {
        throw new IllegalArgumentException("The following feature columns are not available on" +
          s" the training dataset: '${missingColumns.mkString(", ")}'")
      }
    }

    val featureColumns = getFeaturesCols().map(sanitize).map(col)
    val excludedColumns = excludedCols.map(sanitize).map(col)
    val columns = featureColumns ++ excludedColumns
    val h2oContext = H2OContext.ensure("H2OContext needs to be created in order to train the model. Please create one as H2OContext.getOrCreate().")
    val trainFrame = H2OFrame(h2oContext.asH2OFrameKeyString(dataset.select(columns: _*).toDF()))

    // Our MOJO wrapper needs the full column name before the array/vector expansion in order to do predictions
    val internalFeatureCols = SchemaUtils.flattenStructsInDataFrame(dataset.select(featureColumns: _*)).columns
    if (getAllStringColumnsToCategorical()) {
      trainFrame.convertAllStringColumnsToCategorical()
    }
    trainFrame.convertColumnsToCategorical(getColumnsToCategorical())

    if (getSplitRatio() < 1.0) {
      val frames = trainFrame.splitToTrainAndValidationFrames(getSplitRatio())
      if (frames.length > 1) {
        (frames(0), Some(frames(1)), internalFeatureCols)
      } else {
        (frames(0), None, internalFeatureCols)
      }
    } else {
      (trainFrame, None, internalFeatureCols)
    }
  }

  def sanitize(colName: String) = '`' + colName + '`'
}
