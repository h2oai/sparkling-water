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
import org.apache.spark.{SparkContext}
import org.apache.spark.h2o.{SparklingConf, H2OContext}
import org.scalatest.{Suite, BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Helper trait to simplify initialization and termination of Spark/H2O contexts.
 *
 */
trait SparkTestContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _
  @transient var hc: H2OContext = _

  override def beforeAll() {
    System.setProperty("spark.testing", "true")
    sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  def resetContext() = {
    SparkTestContext.stop(sc)
    sc = null
    hc = null
  }

  def defaultSparkConf =  new SparklingConf().set("spark.ext.h2o.disable.ga", "true")
}

/** This fixture create a Spark context once and share it over whole run of test suite. */
trait SharedSparkTestContext extends SparkTestContext { self: Suite =>

  def createSparkContext:SparkContext
  def createH2OContext(sc:SparkContext):H2OContext = H2OContext.getOrCreate(sc)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = createSparkContext
    hc = createH2OContext(sc)
  }

  override protected def afterAll(): Unit = {
    resetContext()
    super.afterAll()
  }
}

/** This fixture create a Spark context once and share it over whole run of test suite.
  *
  * FIXME: this cannot be used yet, since H2OContext cannot be recreated in JVM. */
trait PerTestSparkTestContext extends SparkTestContext { self: Suite =>

  def createSparkContext:SparkContext
  def createH2OContext(sc:SparkContext):H2OContext = H2OContext.getOrCreate(sc)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sc = createSparkContext
    hc = createH2OContext(sc)
  }

  override protected def afterEach(): Unit = {
    resetContext()
    super.afterEach()
  }
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

