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

package org.apache.spark.examples.h2o

import java.net.URI

import hex.Model.Output
import org.apache.spark.SparkContext
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp.show
import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import water.api.TestUtils
import water.support.{ModelSerializationSupport, SparkContextSupport}

/**
 * Variant of Craigslist App with streaming support to classify
 * incoming events.
 *
 * Launch: nc -lk 9999
 * and send events from your console
 *
 * Send "poison pill" to kill the application.
 *
 */
object CraigslistJobTitlesStreamingApp extends SparkContextSupport with ModelSerializationSupport {

  val POISON_PILL_MSG = "poison pill"

  def main(args: Array[String]) {
    // Prepare environment
    val sc = new SparkContext(configure("CraigslistJobTitlesStreamingApp"))
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    // Start H2O services
    val h2oContext = H2OContext.getOrCreate(sc)

    // Build an initial model
    val staticApp = new CraigslistJobTitlesApp()(sc, sqlContext, h2oContext)
    try {
      val (svModel, w2vModel) = staticApp.buildModels(TestUtils.locate("smalldata/craigslistJobTitles.csv"), "initialModel")
      val modelId = svModel._key.toString
      val classNames = svModel._output.asInstanceOf[Output].classNames()

      // Lets save models
      exportSparkModel(w2vModel, URI.create("file:///tmp/sparkmodel"))
      exportH2OModel(svModel, URI.create("file:///tmp/h2omodel/"))

      // Start streaming context
      val jobTitlesStream = ssc.socketTextStream("localhost", 9999)

      // Classify incoming messages
      jobTitlesStream.filter(!_.isEmpty)
        .map(jobTitle => (jobTitle, staticApp.classify(jobTitle, modelId, w2vModel)))
        .map(pred => "\"" + pred._1 + "\" = " + show(pred._2, classNames))
        .print()

      // Shutdown app if poison pill is passed as a message
      jobTitlesStream.filter(msg => POISON_PILL_MSG == msg)
        .foreachRDD(rdd => if (!rdd.isEmpty()) {
          println("Poison pill received! Application is going to shut down...")
          ssc.stop(true, true)
          staticApp.shutdown()
      })

      println("Please start the event producer at port 9999, for example: nc -lk 9999")
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      ssc.stop()
      staticApp.shutdown()
    }
  }

}
