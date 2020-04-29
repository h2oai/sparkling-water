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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionMultinomial.{Base, Detailed}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionOrdinal {
  self: H2OMOJOModel =>
  def getOrdinalPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      udf[Detailed, Row, Double] { (r: Row, offset: Double) =>
        val model = H2OMOJOCache.getMojoBackend(uid, getMojoLocalPath, this)
        val pred = model.predictOrdinal(RowConverter.toH2ORowData(r), offset)
        val probabilities = model.getResponseDomainValues.zip(pred.classProbabilities).toMap
        Detailed(pred.label, probabilities)
      }
    } else {
      udf[Base, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache
          .getMojoBackend(uid, getMojoLocalPath, this)
          .predictOrdinal(RowConverter.toH2ORowData(r), offset)
        Base(pred.label)
      }
    }
  }

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getOrdinalPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getOrdinalDetailedPredictionColSchema(): Seq[StructField] = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)

    val fields = if (getWithDetailedPredictionCol()) {
      val probabilitiesField =
        StructField("probabilities", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true)
      labelField :: probabilitiesField :: Nil
    } else {
      labelField :: Nil
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractOrdinalPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}
