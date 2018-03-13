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
import org.apache.spark.h2o.utils.SparkTestContext
import org.apache.spark.ml.h2o.models.H2OMojoPipelineModel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
@RunWith(classOf[JUnitRunner])
class H2OMojoPipelineModelTest extends FunSuite with SparkTestContext {

  override def beforeAll(): Unit = {
    sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
    super.beforeAll()
  }

  test("Prediction on Mojo Pipeline (Mojo2)") {
    // Test data
    val df = spark.read.option("header", "true").csv("../examples/smalldata/prostate/prostate.csv")
    // Test mojo
    val mojo = H2OMojoPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    val rawMojo = mojo.getOrCreateModel()

    val icolNames = (0 until rawMojo.getInputMeta.size()).map(i => rawMojo.getInputMeta.getColumnName(i))
    val icolTypes = (0 until rawMojo.getInputMeta.size()).map(i => rawMojo.getInputMeta.getColumnType(i))
    val ocolNames = (0 until rawMojo.getOutputMeta.size()).map(i => rawMojo.getOutputMeta.getColumnName(i))
    val ocolTypes = (0 until rawMojo.getOutputMeta.size()).map(i => rawMojo.getOutputMeta.getColumnType(i))
    println("\nMOJO Inputs:")
    println(icolNames.zip(icolTypes).map { case (n,t) => s"${n}[${t}]" }.mkString(", "))
    println("\nMOJO Outputs:")
    println(ocolNames.zip(ocolTypes).map { case (n,t) => s"${n}[${t}]" }.mkString(", "))


    val transDf = mojo.transform(df)
    println(s"\n\nSpark Transformer Output:\n${transDf.dtypes.map { case (n,t) => s"${n}[${t}]" }.mkString(" ")}")
    println("Predictions:")
    println(transDf.select("prediction.preds").take(5).mkString("\n"))
  }
}
