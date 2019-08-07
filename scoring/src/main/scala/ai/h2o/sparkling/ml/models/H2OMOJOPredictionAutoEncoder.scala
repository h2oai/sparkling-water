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
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField}

object H2OMOJOPredictionAutoEncoder extends H2OMOJOPrediction {
  override def getPredictionUDF(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = predictWrapper.predictAutoEncoder(RowConverter.toH2ORowData(r))
      Base(pred.original, pred.reconstructed)
    }
  }

  override def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    getDetailedPredictionColSchema(model, predictWrapper)
  }

  override def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    Seq("original", "reconstructed").map(StructField(_, ArrayType(DoubleType)))
  }

  case class Base(original: Array[Double], reconstructed: Array[Double])

  override def extractSimplePredictionColumns(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column = {
    col(model.getDetailedPredictionCol())
  }
}
