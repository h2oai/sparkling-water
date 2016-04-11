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
package org.apache.spark.h2o.util

import io.netty.util.internal.logging.{Slf4JLoggerFactory, InternalLoggerFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.H2OContext
import org.scalatest.{Suite, BeforeAndAfterAll, BeforeAndAfterEach}

/**
  * Helper trait to simplify initialization and termination of Spark/H2O contexts.
  *
  */
trait SparkTestContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _
  @transient var hc: H2OContext = _
  @transient implicit var sqlc: SQLContext = _

  override def beforeAll() {
    System.setProperty("spark.testing", "true")
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  def resetContext() = {
    SparkTestContext.stop(sc)
    sc = null
    hc = null
  }
  def defaultSparkConf =  new SparkConf()
    .set("spark.ext.h2o.disable.ga", "true")
    .set("spark.driver.memory", "2G")
    .set("spark.executor.memory", "2G")
    .set("spark.app.id", self.getClass.getSimpleName)
    .set("spark.ext.h2o.client.log.level", "DEBUG")
    .set("spark.ext.h2o.repl.enabled","false") // disable repl in tests
    .set("spark.scheduler.minRegisteredResourcesRatio", "1")
}

object SparkTestContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}

