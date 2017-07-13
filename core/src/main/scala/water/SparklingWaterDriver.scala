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

package water

import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.{SparkConf, SparkSessionUtils}

/**
  * A simple wrapper to allow launching H2O itself on the
  * top of Spark.
  */
object SparklingWaterDriver {

  /** Entry point */
  def main(args: Array[String]) {
    // Configure this application
    val conf: SparkConf = H2OConf.checkSparkConf(
      new SparkConf()
        .setAppName("Sparkling Water Driver")
        .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
        .set("spark.ext.h2o.repl.enabled", "true"))

    val spark = SparkSessionUtils.createSparkSession(conf)
    // Start H2O cluster only
    val hc = H2OContext.getOrCreate(spark.sparkContext)

    println(hc)

    // Infinite wait
    this.synchronized(while (true) {
      wait()
    })
  }
}
