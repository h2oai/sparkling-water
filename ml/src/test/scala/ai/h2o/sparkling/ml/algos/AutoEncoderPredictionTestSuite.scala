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

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class AutoEncoderPredictionTestSuite
  extends FunSuite
  with Matchers
  with SharedH2OTestContext
  with TransformSchemaTestSuite {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  override protected lazy val dataset: DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_test.csv"))
  }

  override protected def mojoName: String = "deep_learning_auto_encoder.mojo"

  override protected def expectedDetailedPredictionCol: StructField = {
    val originalField = StructField("original", ArrayType(DoubleType, containsNull = false), nullable = true)
    val reconstructedField = StructField("reconstructed", ArrayType(DoubleType, containsNull = false), nullable = true)
    StructField("detailed_prediction", StructType(originalField :: reconstructedField :: Nil), nullable = true)
  }

  override protected def expectedPredictionCol: StructField = {
    StructField("prediction", ArrayType(DoubleType, containsNull = false), nullable = true)
  }
}
