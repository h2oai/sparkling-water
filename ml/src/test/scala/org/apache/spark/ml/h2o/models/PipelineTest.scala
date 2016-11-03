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
package org.apache.spark.ml.h2o.models

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.FunSuite

class PipelineTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local",
    conf = defaultSparkConf)

  test("Basic pipeline test") {
    val h2oContext = H2OContext.getOrCreate(sc)
    val inp = sc.parallelize(Seq(
      Row(Vectors.dense(1987, 10, 14, 3, 741, 730, 912, 849, 1451, 91, 79, 23, 11, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 15, 4, 729, 730, 903, 849, 1451, 94, 79, 14, -1, 447, 0, 0), 0),
      Row(Vectors.dense(1987, 10, 17, 6, 741, 730, 918, 849, 1451, 97, 79, 29, 11, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 18, 7, 729, 730, 847, 849, 1451, 78, 79, -2, -1, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 19, 1, 749, 730, 922, 849, 1451, 93, 79, 33, 19, 447, 0, 0), 0),
      Row(Vectors.dense(1987, 10, 21, 3, 728, 730, 848, 849, 1451, 80, 79, -1, -2, 447, 0, 0), 1),
      Row(Vectors.dense(1987, 10, 22, 4, 728, 730, 852, 849, 1451, 84, 79, 3, -2, 447, 0, 0), 1)
    ))

    val inpDF = sqlContext.createDataFrame(
      inp,
      StructType(Seq(
        StructField("features", SQLDataTypes.VectorType),
        StructField("label", DataTypes.IntegerType)
      ))
    )

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val airlinesData = h2oContext.asH2OFrame(inpDF)

    airlinesData.replace(airlinesData.numCols() - 1, airlinesData.lastVec().toCategoricalVec)
    airlinesData.update()

    val h2oDL = new H2ODeepLearning()(hc, sqlContext)
      .setEpochs(10)
      .setLabelCol("label")
      .setFeaturesCol("normFeatures")

    val pipeline = new Pipeline().setStages(Array(normalizer, h2oDL))

    val model = pipeline.fit(inpDF)
    val pred = model.transform(inpDF)
    // TODO add assertions
    // TODO add a way to perform modifications on the frame in the pipeline
    pred.foreach(row => println(row))
  }

}
