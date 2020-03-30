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

package ai.h2o.sparkling.examples

import java.io.File

import ai.h2o.sparkling.ml.algos.H2OKMeans
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession

object ProstateDemo {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Prostate Demo")
      .getOrCreate()
    import spark.implicits._
    val prostateDataPath = "./examples/smalldata/prostate/prostate.csv"
    val prostateDataFile = s"file://${new File(prostateDataPath).getAbsolutePath}"

    val prostateTable = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(prostateDataFile)

    // Select a subsample
    val train = prostateTable.filter('CAPSULE === 1)

    H2OContext.getOrCreate()
    val kmeans = new H2OKMeans().setK(3)
    val model = kmeans.fit(train)
    val prediction = model.transform(train).select("prediction").collect()
    println(prediction.mkString("\n===> Model predictions: ", ", ", ", ...\n"))
  }
}
