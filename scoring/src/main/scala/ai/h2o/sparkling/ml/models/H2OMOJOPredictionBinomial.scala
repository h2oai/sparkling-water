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

trait H2OMOJOPredictionBinomial extends PredictionWithContributions {
  self: H2OMOJOModel =>

  private def supportsCalibratedProbabilities(predictWrapper: EasyPredictModelWrapper): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    predictWrapper.m.calibrateClassProbabilities(Array.fill[Double](3)(0))
  }

  def getBinomialPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojo, this))) {
        if (getWithContributions()) {
          udf[DetailedWithContributionsAndCalibration, Row, Double] { (r: Row, offset: Double) =>
            val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
            val pred = model.predictBinomial(RowConverter.toH2ORowData(r), offset)
            val probabilities = model.getResponseDomainValues.zip(pred.classProbabilities).toMap
            val calibratedProbabilities = model.getResponseDomainValues.zip(pred.calibratedClassProbabilities).toMap
            val contributions = convertContributionsToMap(model, pred.contributions)
            DetailedWithContributionsAndCalibration(pred.label, probabilities, contributions, calibratedProbabilities)
          }
        } else {
          udf[DetailedWithCalibration, Row, Double] { (r: Row, offset: Double) =>
            val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
            val pred = model.predictBinomial(RowConverter.toH2ORowData(r), offset)
            val probabilities = model.getResponseDomainValues.zip(pred.classProbabilities).toMap
            val calibratedProbabilities = model.getResponseDomainValues.zip(pred.calibratedClassProbabilities).toMap
            DetailedWithCalibration(pred.label, probabilities, calibratedProbabilities)
          }
        }
      } else {
        if (getWithContributions()) {
           udf[DetailedWithContributions, Row, Double] { (r: Row, offset: Double) =>
            val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
            val pred = model.predictBinomial(RowConverter.toH2ORowData(r), offset)
            val probabilities = model.getResponseDomainValues.zip(pred.classProbabilities).toMap
            val contributions = convertContributionsToMap(model, pred.contributions)
            DetailedWithContributions(pred.label, probabilities, contributions)
          }
        } else {
          udf[Detailed, Row, Double] { (r: Row, offset: Double) =>
            val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
            val pred = model.predictBinomial(RowConverter.toH2ORowData(r), offset)
            val probabilities = model.getResponseDomainValues.zip(pred.classProbabilities).toMap
            Detailed(pred.label, probabilities)
          }
        }
      }
    } else {
      udf[Base, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache
          .getMojoBackend(uid, getMojo, this)
          .predictBinomial(RowConverter.toH2ORowData(r), offset)
        Base(pred.label)
      }
    }
  }

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getBinomialPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getBinomialDetailedPredictionColSchema(): Seq[StructField] = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)
    val baseFields = labelField :: Nil

    val fields = if (getWithDetailedPredictionCol()) {
      val probabilitiesField =
        StructField("probabilities", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true)
      val detailedPredictionFields = baseFields :+ probabilitiesField

      val contributionsFields = if (getWithContributions()) {
        val contributionsField = StructField("contributions", getContributionsSchema(), nullable = true)
        detailedPredictionFields :+ contributionsField
      } else {
        detailedPredictionFields
      }

      if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojo, this))) {
        val calibratedProbabilitiesField = StructField(
          "calibratedProbabilities",
          MapType(StringType, DoubleType, valueContainsNull = false),
          nullable = false)
        contributionsFields :+ calibratedProbabilitiesField
      } else {
        contributionsFields
      }
    } else {
      baseFields
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractBinomialPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}

object H2OMOJOPredictionBinomial {

  case class Base(label: String)

  case class Detailed(label: String, probabilities: Map[String, Double])

  case class DetailedWithContributions(
      label: String,
      probabilities: Map[String, Double],
      contributions: Map[String, Float])

  case class DetailedWithCalibration(
      label: String,
      probabilities: Map[String, Double],
      calibratedProbabilities: Map[String, Double])

  case class DetailedWithContributionsAndCalibration(
      label: String,
      probabilities: Map[String, Double],
      contributions: Map[String, Float],
      calibratedProbabilities: Map[String, Double])

}
