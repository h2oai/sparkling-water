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
package org.apache.spark.h2o.utils

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

/** This fixture create a Spark context once and share it over whole run of test suite.
  *
  * FIXME: this cannot be used yet, since H2OContext cannot be recreated in JVM. */
trait PerTestSparkTestContext extends SparkTestContext { self: Suite =>

  def createSparkContext:SparkContext
  def createH2OContext(sc: SparkContext, conf: H2OConf):H2OContext = H2OContext.getOrCreate(sc)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sc = createSparkContext
    sqlc = SQLContext.getOrCreate(sc)

    if(testsInExternalMode(sc.getConf)){
      startCloud(2, sc.getConf)
    }
    hc = createH2OContext(sc, new H2OConf(sc).setNumOfExternalH2ONodes(2))
  }

  override protected def afterEach(): Unit = {
    if(testsInExternalMode(sc.getConf)){
      stopCloud()
    }
    resetContext()
    super.afterEach()
  }
}
