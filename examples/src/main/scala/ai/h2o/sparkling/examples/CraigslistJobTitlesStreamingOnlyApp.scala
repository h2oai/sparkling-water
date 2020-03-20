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

import java.net.URI

import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import water.support.{ModelSerializationSupport, SparkContextSupport}

/**
 * Streaming app using saved models.
 */
object CraigslistJobTitlesStreamingOnlyApp extends SparkContextSupport with ModelSerializationSupport {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("CraigslistJobTitlesStreamingOnlyApp").getOrCreate()
    val hc = H2OContext.getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val staticApp = new CraigslistJobTitlesApp()(spark, hc)

    try {
      val svModel = loadSparkModel[H2OMOJOModel](URI.create("file:///tmp/h2omodel"))
      val w2vModel = loadSparkModel[Word2VecModel](URI.create("file:///tmp/sparkmodel"))

      // Start streaming context
      val jobTitlesStream = ssc.socketTextStream("localhost", 9999)

      // Classify incoming messages
      jobTitlesStream.filter(!_.isEmpty)
        .map(jobTitle => (jobTitle, staticApp.predict(jobTitle, svModel, w2vModel)))
        .print()

      println("Please start the event producer at port 9999, for example: nc -lk 9999")
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      ssc.stop()
      spark.stop()
      hc.stop()
    }
  }
}
