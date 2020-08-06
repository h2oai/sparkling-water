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

import ai.h2o.sparkling.ml.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionOrdinal {
  self: H2OMOJOModel =>
  def getOrdinalPredictionUDF(): UserDefinedFunction = {
    val schema = getOrdinalPredictionSchema()
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
      val pred = model.predictOrdinal(RowConverter.toH2ORowData(r), offset)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.label
      resultBuilder += Utils.arrayToRow(pred.classProbabilities)
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getOrdinalPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getOrdinalPredictionSchema(): StructType = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)

    val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    val classFields = model.getResponseDomainValues.map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField = StructField("probabilities", StructType(classFields), nullable = false)
    val fields = labelField :: probabilitiesField :: Nil

    StructType(fields)
  }

  def extractOrdinalPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}
