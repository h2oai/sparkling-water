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
package org.apache.spark.ml.spark.models.gm

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class GaussianMixtureTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf
    = defaultSparkConf)

  test("Should predict the same cluster as the spark method.") {
    val h2oContext = hc
    import h2oContext.implicits._

    // Generate random training data
    val trainRDD = sc.parallelize(1 to 50, 1).map(v => {
      val features: Array[Double] = Array.fill(5) {0}.map(x => Random.nextDouble())
      Row(features: _*)
    }).cache()
    val trainDF: DataFrame = sqlContext.createDataFrame(
      trainRDD,
      StructType(Array(
        StructField("c0", DataTypes.DoubleType),
        StructField("c1", DataTypes.DoubleType),
        StructField("c2", DataTypes.DoubleType),
        StructField("c3", DataTypes.DoubleType),
        StructField("c4", DataTypes.DoubleType)
      ))
    )

    val trainFrame = hc.asH2OFrame(trainDF, "bubbles")

    // Learning parameters
    val parms = new GaussianMixtureParameters
    parms._train = trainFrame
    parms._response_column = 'Label
    parms._k = 3

    val gm = new GaussianMixture(parms, h2oContext)

    // Train model
    val h2oGMModel = gm.trainModel.get

    val sparkGMModel = h2oGMModel.sparkModel

    // Make sure both scoring methods return the same results
    val h2oPredsVec = h2oGMModel.score(trainFrame).vec(0)
    val h2oPreds = (0 until 50).map(h2oPredsVec.at(_)).toArray
    val sparkPreds = sparkGMModel.predict(
      trainRDD
        .map(row => {
          Vectors.dense(row.toSeq.map(_.asInstanceOf[Double]).toArray)
        })
    ).collect().map(_.toDouble)

    assert(h2oPreds.sameElements(sparkPreds))

  }
}
