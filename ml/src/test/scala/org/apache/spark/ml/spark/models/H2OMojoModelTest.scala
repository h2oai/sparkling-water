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

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OMojoModelTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext = new SparkContext("local[*]", "mojo-test-local", conf = defaultSparkConf)

  // TODO:
  //
  //
  //   -  // WRONG this patter needs to share the same code as in the data transformation (renaming of columns for vectors
  // needs to be unified
  test("[MOJO] Export and Import") {
    val inputDf = hc.asDataFrame(new H2OFrame(URI.create("examples/smalldata/prostate.csv")))
    val gbm = new H2OGBM()(hc, sqlContext)
    gbm.setPredictionsCol("capsule")
    val model = gbm.fit(inputDf)
    model.write.overwrite.save("/tmp/xxx")
    val reloadedModel  = H2OMOJOModel.load("/tmp/xxx")
    val prediction = reloadedModel.transform(inputDf)
    prediction.show()
    // Build gm pipeline model, save and reload

  }
  
  // More tests:
  // - save mojo as raw file and create H2OMojoModel from it directly
}
