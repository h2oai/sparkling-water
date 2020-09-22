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

package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class WordEmbeddingTestSuite extends FunSuite with Matchers with SharedH2OTestContext with TransformSchemaTestSuite {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  override protected lazy val dataset: DataFrame = {
    Seq(
      "Lorem ipsum dolor sit amet, consectetuer adipiscing elit",
      "Curabitur vitae diam non enim vestibulum interdum",
      "Fusce suscipit libero eget elit",
      "Nunc auctor").toDF("text")
  }

  override protected def mojoName: String = "word2vec.mojo"

  private def predictionColType = {
    ArrayType(FloatType, containsNull = false)
  }

  override protected def expectedDetailedPredictionCol: StructField = {
    val wordEmbeddingsField = StructField("wordEmbeddings", predictionColType, nullable = true)
    StructField("detailed_prediction", StructType(wordEmbeddingsField :: Nil), nullable = true)
  }

  override protected def expectedPredictionCol: StructField = {
    val predictionColType = ArrayType(FloatType, containsNull = false)
    StructField("prediction", predictionColType, nullable = true)
  }
}
