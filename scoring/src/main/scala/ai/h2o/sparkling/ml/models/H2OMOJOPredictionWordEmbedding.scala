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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionWordEmbedding.Base
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait H2OMOJOPredictionWordEmbedding {
  self: H2OMOJOModel =>

  def getWordEmbeddingPredictionUDF(): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this).predictWord2Vec(RowConverter.toH2ORowData(r))
      Base(pred.wordEmbeddings.asScala)
    }
  }

  private val predictionColType = DataTypes.createMapType(StringType, ArrayType(FloatType, containsNull = false))
  private val predictionColNullable = true

  def getWordEmbeddingPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getWordEmbeddingDetailedPredictionColSchema(): Seq[StructField] = {
    val fields = StructField("wordEmbeddings", predictionColType, nullable = predictionColNullable) :: Nil

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractWordEmbeddingPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.wordEmbeddings")
  }

}

object H2OMOJOPredictionWordEmbedding {

  case class Base(wordEmbeddings: mutable.Map[String, Array[Float]])

}
