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

package ai.h2o.sparkling.local

import ai.h2o.sparkling.LocalIntegrationTest
import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.support.SparkContextSupport

@RunWith(classOf[JUnitRunner])
class PubDev457Suite extends LocalIntegrationTest {

  test("Simple ML pipeline using H2O") {
    launch(PubDev457Test)
  }
}

object PubDev457Test extends SparkContextSupport {

  case class LabeledDocument(id: Long, text: String, label: Double)

  case class Document(id: Long, text: String)

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-457")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext.implicits._
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    import sqlContext.implicits._

    val training = sc.parallelize(Seq(
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0)))

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))
    val model = pipeline.fit(training.toDF)
    val transformed = model.transform(training.toDF)

    val transformedDF: H2OFrame = transformed
    assert(transformedDF.numRows == 4)
    assert(transformedDF.numCols == 1009)

    val transformedFeaturesDF: H2OFrame = transformed.select("features")
    assert(transformedFeaturesDF.numRows == 4)
    assert(transformedFeaturesDF.numCols == 1000)

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
