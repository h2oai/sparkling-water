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

package ai.h2o.sparkling

import java.net.URI

import org.apache.spark.sql.SparkSession

object InitTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("App name").getOrCreate()
    import spark.implicits._
    val hc = H2OContext.getOrCreate()
    val expected = spark.sparkContext.getConf.get("spark.executor.instances").toInt
    val actual = hc.getH2ONodes().length
    if (actual != expected) {
      throw new RuntimeException(s"H2O cluster should be of size $expected but is $actual")
    }
    val frame = H2OFrame(
      new URI(
        "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"))
    val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
    val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

    import ai.h2o.sparkling.ml.algos.H2OXGBoost
    val estimator = new H2OXGBoost().setLabelCol("CAPSULE")
    val model = estimator.fit(trainingDF)

    model.transform(testingDF).show(false)
  }
}
