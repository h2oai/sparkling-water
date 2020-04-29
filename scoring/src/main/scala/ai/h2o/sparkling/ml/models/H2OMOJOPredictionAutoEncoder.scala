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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionAutoEncoder.{Base, Detailed}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionAutoEncoder {
  self: H2OMOJOModel =>

  def getAutoEncoderPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      udf[Detailed, Row] { r: Row =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoLocalPath, this).predictAutoEncoder(RowConverter.toH2ORowData(r))
        Detailed(pred.original, pred.reconstructed)
      }
    } else {
      udf[Base, Row] { r: Row =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoLocalPath, this).predictAutoEncoder(RowConverter.toH2ORowData(r))
        Base(pred.original)
      }
    }

  }

  private val predictionColType = ArrayType(DoubleType, containsNull = false)
  private val predictionColNullable = true

  def getAutoEncoderPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getAutoEncoderDetailedPredictionColSchema(): Seq[StructField] = {
    val originalField = StructField("original", predictionColType, nullable = predictionColNullable)
    val fields = if (getWithDetailedPredictionCol()) {
      val reconstructedField = StructField("reconstructed", predictionColType, nullable = predictionColNullable)
      originalField :: reconstructedField :: Nil
    } else {
      originalField :: Nil
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractAutoEncoderPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.original")
  }
}

object H2OMOJOPredictionAutoEncoder {

  case class Base(original: Array[Double])

  case class Detailed(original: Array[Double], reconstructed: Array[Double])

}
