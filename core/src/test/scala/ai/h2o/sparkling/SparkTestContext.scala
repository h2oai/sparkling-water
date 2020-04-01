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

package ai.h2o.sparkling

import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.h2o.H2OConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import water.init.NetworkInit

/**
  * Helper trait to simplify initialization and termination of Spark contexts.
  */
trait SparkTestContext extends BeforeAndAfterAll {
  self: Suite =>

  def sparkSession(master: String, conf: SparkConf): SparkSession = {
    SparkSession.builder().config(conf).master(master).appName(getClass.getName).getOrCreate()
  }

  def sparkSession(master: String): SparkSession = {
    sparkSession(master, defaultSparkConf)
  }

  def createSparkSession(): SparkSession

  @transient private var sparkInternal: SparkSession = _
  @transient lazy val spark: SparkSession = sparkInternal
  @transient lazy val sc: SparkContext = spark.sparkContext

  override def beforeAll() {
    sparkInternal = createSparkSession()
    System.setProperty("spark.testing", "true")
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
    super.beforeAll()
  }

  def defaultSparkConf: SparkConf =
    H2OConf.checkSparkConf({
      val conf = new SparkConf()
        .set("spark.ext.h2o.cloud.name", getClass.getSimpleName)
        .set("spark.driver.memory", "1G")
        .set("spark.executor.memory", "1G")
        .set("spark.app.id", getClass.getSimpleName)
        .set("spark.ext.h2o.client.log.level", "DEBUG")
        .set("spark.ext.h2o.repl.enabled", "false") // disable repl in tests
        .set("spark.scheduler.minRegisteredResourcesRatio", "1")
        .set(
          "spark.ext.h2o.backend.cluster.mode",
          sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal"))
        .set(
          "spark.ext.h2o.client.ip",
          sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))
        .set("spark.ext.h2o.external.start.mode", "auto")
      conf
    })
}
