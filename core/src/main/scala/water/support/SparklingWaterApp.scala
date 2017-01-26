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
package water.support

import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext

import scala.annotation.meta.getter

/**
 * A simple application trait to define Sparkling Water applications.
 */
trait SparklingWaterApp {

  @(transient @getter) val sc: SparkContext
  @(transient @getter) val sqlContext: SQLContext
  @(transient @getter) val h2oContext: H2OContext

  def loadH2OFrame(datafile: String) = new H2OFrame(new java.net.URI(datafile))

  def shutdown(): Unit = {
    // Shutdown Spark
    sc.stop()
    // Shutdown H2O explicitly (at least the driver)
    h2oContext.stop()
  }
}
