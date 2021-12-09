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

import ai.h2o.sparkling.examples.CraigslistJobTitlesApp._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.SparkSession

/**
  * Variant of Craigslist App with structured streaming support to classify incoming events.
  *
  * Launch: nc -lk 9999 and send events from your console
  *
  */
object CraigslistJobTitlesStructuredStreamingApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Craigslist Job Titles Structured Streaming App")
      .getOrCreate()

    val titlesTable = loadTitlesTable(spark)
    val model = fitModelPipeline(titlesTable)

    // consume data from socket
    val dataStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // bring encoders into scope
    import spark.implicits._

    // interpret input data as job titles
    val jobTitlesStream = dataStream
      .as[String]
      .withColumnRenamed("value", "jobtitle")

    // use model to predict category
    val prediction = model.transform(jobTitlesStream)

    // select relevant output columns
    val categoryPrediction = prediction.select("jobtitle", "prediction", "detailed_prediction.probabilities.*")

    // start streaming query, put output to console
    val query = categoryPrediction.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
