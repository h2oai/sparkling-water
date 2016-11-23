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
package ai.h2o

import org.apache.spark.streaming._
import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import water.support.SparkContextSupport

object PipelineDemo extends SparkContextSupport {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PipelineDemo <port>")
      System.exit(1)
    }
    val port = args(0).toInt

    val sparkConf = configure("PipelineDemo")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val hc = H2OContext.getOrCreate(ssc.sparkContext)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    import hc._
    import hc.implicits._

    val lines = ssc.socketTextStream("localhost", port)
    lines.print() // useful to see some of the data stream for debugging
    val events = lines.map(_.split(",")).map(
      e=> RandomEvent(e(0), e(1).toDouble, e(2).toDouble)
    )
    var hf: H2OFrame = null
    events.window(Seconds(300), Seconds(10)).foreachRDD(rdd =>
      {
        if (!rdd.isEmpty ) {
          try {
            hf.delete()
          } catch { case e: Exception => println("Initialized frame") }
          hf = hc.asH2OFrame(rdd.toDF(), "demoFrame10s") //the name of the frame
          // perform your munging, score your events with a pretrained model or
          // mini-batch training on checkpointed models, etc
          // make sure your execution finishes within the batch cycle (the
          // second arg in the window)
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
