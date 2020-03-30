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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

class AnomalyPredictionTestSuite
  extends FunSuite
  with Matchers
  with SharedH2OTestContext
  with TransformSchemaTestSuite {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  override protected lazy val dataset: DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
  }

  override protected def mojoName: String = "isolation_forest_prostate.mojo"

  override protected def expectedDetailedPredictionCol: StructField = {
    val scoreField = StructField("score", DoubleType, nullable = false)
    val normalizedScoreField = StructField("normalizedScore", DoubleType, nullable = false)
    StructField("detailed_prediction", StructType(scoreField :: normalizedScoreField :: Nil), nullable = true)
  }

  override protected def expectedPredictionCol: StructField = {
    StructField("prediction", DoubleType, nullable = true)
  }
}
