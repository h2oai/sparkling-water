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

import scala.annotation.meta.{field, getter}

/**
 * A simple application trait to define Sparkling Water applications.
 */
trait SparklingWaterApp {

  @(transient @field @getter) val sc: SparkContext
  @(transient @field @getter) val sqlContext: SQLContext
  @(transient @field @getter) val h2oContext: H2OContext

  /**
    * Create H2OFrame from specified data file
    * @param datafile path to the file
    * @return created H2O Frame
    */
  def loadH2OFrame(datafile: String) = new H2OFrame(new java.net.URI(datafile))

  /**
    * Shutdown the application
    */
  def shutdown(): Unit = {
    // Shutdown Spark
    sc.stop()
    // Shutdown H2O explicitly
    h2oContext.stop()
  }
}
