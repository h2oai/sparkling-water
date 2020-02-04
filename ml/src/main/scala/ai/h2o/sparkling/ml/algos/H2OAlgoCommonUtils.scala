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

import ai.h2o.sparkling.ml.params.H2OCommonParams
import ai.h2o.sparkling.ml.utils.SchemaUtils
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.external.RestApiUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import water.support.H2OFrameSupport
import water.{DKV, Key}

trait H2OAlgoCommonUtils extends H2OCommonParams {

  protected def prepareDatasetForFitting(dataset: Dataset[_]): (String, Option[String], Array[String]) = {
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

    val cols = (getFeaturesCols() ++ excludedCols).map(col)
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val h2oFrameKey = h2oContext.asH2OFrameKeyString(dataset.select(cols: _*).toDF())

    // Our MOJO wrapper needs the full column name before the array/vector expansion in order to do predictions
    val internalFeatureCols = SchemaUtils.flattenStructsInDataFrame(dataset.select(getFeaturesCols().map(col): _*)).columns
    if (getAllStringColumnsToCategorical()) {
      if (RestApiUtils.isRestAPIBased()) {
        RestApiUtils.convertAllStringVecToCategorical(h2oContext.getConf, h2oFrameKey)
      } else {
        H2OFrameSupport.allStringVecToCategorical(DKV.getGet(h2oFrameKey))
      }
    }

    if (RestApiUtils.isRestAPIBased()) {
      RestApiUtils.convertColumnsToCategorical(h2oContext.getConf, h2oFrameKey, getColumnsToCategorical())
    } else {
      H2OFrameSupport.columnsToCategorical(DKV.getGet(h2oFrameKey), getColumnsToCategorical())
    }

    if (getSplitRatio() < 1.0) {
      // need to do splitting
      val keys = if (RestApiUtils.isRestAPIBased()) {
        RestApiUtils.splitFrameToTrainAndValidationFrames(h2oContext.getConf, h2oFrameKey, getSplitRatio())
      } else {
        H2OFrameSupport.split(DKV.getGet(h2oFrameKey), Seq(Key.rand(), Key.rand()), Seq(getSplitRatio())).map(_._key.toString)
      }
      if (keys.length > 1) {
        (keys(0), Some(keys(1)), internalFeatureCols)
      } else {
        (keys(0), None, internalFeatureCols)
      }
    } else {
      (h2oFrameKey, None, internalFeatureCols)
    }
  }
}
