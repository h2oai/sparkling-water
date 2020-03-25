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

import ai.h2o.sparkling.ml.algos.H2ODeepLearning
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession

object DeepLearningDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Deep Learning Demo on Airlines Data")
      .getOrCreate()
    import spark.implicits._

    val airlinesDataPath = "./examples/smalldata/airlines/allyears2k_headers.csv"
    val airlinesDataFile = s"file://${new File(airlinesDataPath).getAbsolutePath}"
    val airlinesTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .csv(airlinesDataFile)

    println(s"\n===> Number of all flights: ${airlinesTable.count()}\n")

    val airlinesSFO = airlinesTable.filter('Dest === "SFO")
    println(s"\n===> Number of flights with destination in SFO: ${airlinesSFO.count()}\n")

    println("\n====> Running DeepLearning on the prepared data frame\n")

    val train = airlinesSFO.select('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed)

    H2OContext.getOrCreate()
    val dl = new H2ODeepLearning()
      .setLabelCol("IsDepDelayed")
      .setConvertUnknownCategoricalLevelsToNa(true)
    val count = train.count()
    val model = dl.fit(train.repartition(count.toInt + 10)) // verify also fitting on empty partitions

    val predictions = model.transform(airlinesSFO).select("prediction").collect()
    println(predictions.mkString("\n===> Model predictions from DL: ", ", ", ", ...\n"))
  }
}
