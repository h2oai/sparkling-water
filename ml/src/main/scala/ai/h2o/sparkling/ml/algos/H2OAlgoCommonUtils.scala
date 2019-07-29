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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import water.Key
import water.fvec.Frame
import water.support.H2OFrameSupport

trait H2OAlgoCommonUtils extends H2OCommonParams {
  protected def prepareDatasetForFitting(dataset: Dataset[_]): (Frame, Option[Frame]) = {
    val excludedCols = getExcludedCols()

    // if this is left empty select
    if ($(featuresCols).isEmpty) {
      val features = dataset.columns.filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
      setFeaturesCols(features)
    }

    val cols = (getFeaturesCols() ++ excludedCols).map(col)
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val input = h2oContext.asH2OFrame(dataset.select(cols: _*).toDF())

    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(input)
    }
    H2OFrameSupport.columnsToCategorical(input, getColumnsToCategorical())

    if (getSplitRatio() < 1.0) {
      // need to do splitting
      val keys = H2OFrameSupport.split(input, Seq(Key.rand(), Key.rand()), Seq(getSplitRatio()))
      if (keys.length > 1) {
        (keys(0), Some(keys(1)))
      } else {
        (keys(0), None)
      }
    } else {
      (input, None)
    }
  }
}
