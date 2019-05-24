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

package ai.h2o.sparkling.benchmarks

import org.apache.spark.SparkConf
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession

trait SparklingApp {
  val spark = SparkSession
    .builder()
    .config(createSparkConf())
    .getOrCreate()

  val hc = H2OContext.getOrCreate(spark)

  def createSparkConf(): SparkConf = {
    val conf = new SparkConf()

    // If master is not defined in system properties or environment variables, fallback to local.
    val master = conf.get("spark.master", "local")
    conf.setMaster(master)

    // If the application name is not defined in system properties or environment variables,
    // set it to the class name.
    val appName = conf.get("spark.app.name", this.getClass.getSimpleName)
    conf.setAppName(appName)

    conf
  }

  def main(args: Array[String]): Unit = hc.stop(stopSparkContext = true)
}
