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

package org.apache.spark.ml.spark.models

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.h2o.algos.H2OGLM
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OAlgoTest extends FunSuite with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", "mojo-test-local", conf = defaultSparkConf)

  test("Test H2OGLM Pipeline") {

    val dataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
      // Create GBM model
     val algo = new H2OGLM()(hc, spark.sqlContext).
        setTrainRatio(0.8).
        setSeed(1).
        setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA" , "VOL", "GLEASON").
        setPredictionCol("AGE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/glm_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/glm_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/glm_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/glm_pipeline_model")

    loadedModel.transform(dataset).count()
  }

}
