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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionAnomaly.Base
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionAnomaly {
  self: H2OMOJOModel =>
  def getAnomalyPredictionUDF(): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = easyPredictModelWrapper.predictAnomalyDetection(RowConverter.toH2ORowData(r))
      Base(pred.score, pred.normalizedScore)
    }
  }

  def getAnomalyPredictionColSchema(): Seq[StructField] = {
    getDetailedPredictionColSchema()
  }

  def getAnomalyDetailedPredictionColSchema(): Seq[StructField] = {
    Seq("score", "normalizedScore").map(StructField(_, DoubleType, nullable = false))
  }

  def extractAnomalyPredictionColContent(): Column = {
    col(getDetailedPredictionCol())
  }
}

object H2OMOJOPredictionAnomaly {

  case class Base(score: Double, normalizedScore: Double)

}
