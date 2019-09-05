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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionBinomial._
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionBinomial {
  self: H2OMOJOModel =>

  private def supportsCalibratedProbabilities(predictWrapper: EasyPredictModelWrapper): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    predictWrapper.m.calibrateClassProbabilities(Array.fill[Double](3)(0))
  }

  def getBinomialPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      if (supportsCalibratedProbabilities(easyPredictModelWrapper)) {
        udf[DetailedWithCalibration, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          DetailedWithCalibration(
            pred.label,
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.contributions,
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1)
          )
        }
      } else {
        udf[Detailed, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          Detailed(
            pred.label,
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.contributions
          )
        }
      }
    } else {
      udf[Base, Row] { r: Row =>
        val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
        Base(pred.label)
      }
    }
  }

  private val predictionColType = StringType
  private val predictionColNullable = false

  def getBinomialPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getBinomialDetailedPredictionColSchema(): Seq[StructField] = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)

    val fields = if (getWithDetailedPredictionCol()) {
      val probabilitiesFields = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))
      val contributionsField = StructField("contributions", ArrayType(FloatType))
      if (supportsCalibratedProbabilities(easyPredictModelWrapper)) {
        val calibratedProbabilitiesFields = Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
        Seq(labelField) ++ probabilitiesFields ++ Seq(contributionsField) ++ calibratedProbabilitiesFields
      } else {
        Seq(labelField) ++ probabilitiesFields ++ Seq(contributionsField)
      }
    } else {
      labelField :: Nil
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = false))
  }

  def extractBinomialPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}

object H2OMOJOPredictionBinomial {

  case class Base(label: String)

  case class Detailed(label: String, p0: Double, p1: Double, contributions: Array[Float])

  case class DetailedWithCalibration(label: String, p0: Double, p1: Double, contributions: Array[Float], p0_calibrated: Double, p1_calibrated: Double)

}
