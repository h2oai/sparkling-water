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

import java.util

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionWordEmbedding.Base
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionWordEmbedding {
  self: H2OMOJOModel =>

  def getWordEmbeddingPredictionUDF(): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = easyPredictModelWrapper.predictWord2Vec(RowConverter.toH2ORowData(r))
      Base(pred.wordEmbeddings)
    }
  }

  def getWordEmbeddingPredictionColSchema(): Seq[StructField] = {
    getWordEmbeddingDetailedPredictionColSchema()
  }

  def getWordEmbeddingDetailedPredictionColSchema(): Seq[StructField] = {
    StructField("wordEmbeddings", DataTypes.createMapType(StringType, ArrayType(FloatType))) :: Nil
  }

  def extractWordEmbeddingPredictionColContent(): Column = {
    col(getDetailedPredictionCol())
  }

}

object H2OMOJOPredictionWordEmbedding {
  case class Base(wordEmbeddings: util.HashMap[String, Array[Float]])
}
