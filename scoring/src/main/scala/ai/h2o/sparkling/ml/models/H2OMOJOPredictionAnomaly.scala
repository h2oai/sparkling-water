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

import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField}

object H2OMOJOPredictionAnomaly extends H2OMOJOPrediction {
  override def getPredictionUDF(model: H2OMOJOModel, predictWrapper: EasyPredictModelWrapper): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = predictWrapper.predictAnomalyDetection(RowConverter.toH2ORowData(r))
      Base(pred.score, pred.normalizedScore)
    }
  }

  override def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    getDetailedPredictionColSchema(model, predictWrapper)
  }

  override def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    Seq("score", "normalizedScore").map(StructField(_, DoubleType, nullable = false))
  }

  case class Base(score: Double, normalizedScore: Double)

  override def extractSimplePredictionColumns(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column = {
    col(model.getDetailedPredictionCol())
  }
}
