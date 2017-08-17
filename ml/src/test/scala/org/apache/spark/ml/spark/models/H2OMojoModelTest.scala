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

import hex.genmodel.utils.DistributionFamily
import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.sql.DataFrame
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
  test("[MOJO] Export and Import - binomial model") {
    val (inputDf, model) = binomialModelFixture
    testModelReload("binomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - multinomial model") {
    val (inputDf, model) = multinomialModelFixture
    testModelReload("multinomial_model_import_export", inputDf, model)
  }

  test("[MOJO] Export and Import - regression model") {
    val (inputDf, model) = regressionModelFixture
    testModelReload("regression_model_import_export", inputDf, model)
  }

  def testModelReload(name: String, df: DataFrame, model: H2OMOJOModel): Unit = {
    val predBeforeSave = model.transform(df)
    val modelFolder = tempFolder(name)
    model.write.overwrite.save(modelFolder)
    val reloadedModel  = H2OMOJOModel.load(modelFolder)
    val predAfterReload = reloadedModel.transform(df)
    predAfterReload.show(150)

    assertEqual(predBeforeSave, predAfterReload)
  }

  // Note: this comparision expects implicit ordering of spark DataFrames which is not ensured!
  def assertEqual(df1: DataFrame, df2: DataFrame, msg: String = "DataFrames are not same!"): Unit = {
    val l1 = df1.repartition(1).collect()
    val l2 = df2.repartition(1).collect()

    assert(l1.zip(l2).forall { case (row1, row2) =>
        row1.equals(row2)
    }, "DataFrames are not same!")

  }

  def tempFolder(prefix: String) = {
    val path = java.nio.file.Files.createTempDirectory(prefix)
    path.toFile.deleteOnExit()
    path.toString
  }

  def irisDataFrame = {
    hc.asDataFrame(new H2OFrame(URI.create("examples/smalldata/iris.csv")))
  }

  def prostateDataFrame = {
    hc.asDataFrame(new H2OFrame(URI.create("examples/smalldata/prostate.csv")))
  }

  def binomialModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()(hc, sqlContext)
    gbm.setParams(p => {
      p._ntrees = 2
      p._distribution = DistributionFamily.bernoulli
    })
    gbm.setPredictionsCol("capsule")
    (inputDf, gbm.fit(inputDf))
  }

  def multinomialModelFixture() = {
    val inputDf = irisDataFrame
    val gbm = new H2OGBM()(hc, sqlContext)
    gbm.setParams(p => {
      p._ntrees = 200
      p._distribution = DistributionFamily.multinomial
    })
    gbm.setPredictionsCol("class")
    (inputDf, gbm.fit(inputDf))
  }

  def regressionModelFixture() = {
    val inputDf = prostateDataFrame
    val gbm = new H2OGBM()(hc, sqlContext)
    gbm.setParams(p => {
      p._ntrees = 2
    })
    gbm.setPredictionsCol("capsule")
    (inputDf, gbm.fit(inputDf))
  }
  
  // More tests:
  // - save mojo as raw file and create H2OMojoModel from it directly
}
