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
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StructField}

object H2OMOJOPredictionRegression extends H2OMOJOPrediction {

  override def getPredictionUDF(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): UserDefinedFunction = {
    if (model.getWithDetailedPredictionCol()) {
      udf[WithContributions, Row] { r: Row =>
        val pred = predictWrapper.predictRegression(RowConverter.toH2ORowData(r))
        WithContributions(pred.value, pred.contributions)
      }
    } else {
      udf[Base, Row] { r: Row =>
        val pred = predictWrapper.predictRegression(RowConverter.toH2ORowData(r))
        Base(pred.value)
      }
    }
  }

  override def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    StructField("value", DoubleType) :: Nil
  }

  override def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    if (model.getWithDetailedPredictionCol()) {
      StructField("value", DoubleType) :: StructField("contributions", ArrayType(FloatType)) :: Nil
    } else {
      StructField("value", DoubleType) :: Nil
    }
  }

  case class Base(value: Double)

  case class WithContributions(value: Double, contributions: Array[Float])

  override def extractSimplePredictionColumns(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column = {
    struct(col(s"${model.getDetailedPredictionCol()}.value")).as("value")
  }
}
