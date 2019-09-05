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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionMultinomial.Base
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionMultinomial {
  self: H2OMOJOModel =>
  def getMultinomialPredictionUDF(): UserDefinedFunction = {
    logWarning("Starting from the next major release, the content of 'prediction' column will be generated to " +
      " 'detailed_prediction' instead. The 'prediction' column will contain directly the predicted label.")
    udf[Base, Row] { r: Row =>
      val pred = easyPredictModelWrapper.predictMultinomial(RowConverter.toH2ORowData(r))
      Base(pred.classProbabilities)
    }
  }

  private val baseFields = StructField("probabilities", ArrayType(DoubleType)) :: Nil

  def getMultinomialPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), StructType(baseFields), nullable = false))
  }

  def getMultinomialDetailedPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getDetailedPredictionCol(), StructType(baseFields), nullable = false))
  }

  def extractMultinomialPredictionColContent(): Column = {
    extractColumnsAsNested(Seq("probabilities"))
  }
}

object H2OMOJOPredictionMultinomial {

  case class Base(probabilities: Array[Double])

}
