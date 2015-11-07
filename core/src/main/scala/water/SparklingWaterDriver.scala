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

import org.apache.spark.h2o.H2OContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A simple wrapper to allow launching H2O itself on the
 * top of Spark.
 */
object SparklingWaterDriver {

  /** Entry point */
  def main(args: Array[String]) {
    // Configure this application
    val conf: SparkConf = new SparkConf().setAppName("Sparkling Water")
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    // Start H2O cluster only
    new H2OContext(sc).start()

    // Infinite wait
    this.synchronized(while (true) {
      wait
    })
  }
}