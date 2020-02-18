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
package org.apache.spark.network

import ai.h2o.sparkling.backend.shared.SharedBackendConf
import org.apache.spark.h2o.H2OConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import water.network.SparklingWaterSecurityUtils

object Security extends Logging {

  def enableSSL(spark: SparkSession, conf: SparkConf): Unit = {
    val sslPair = SparklingWaterSecurityUtils.generateSSLPair()
    val config = SparklingWaterSecurityUtils.generateSSLConfig(sslPair)
    conf.set(SharedBackendConf.PROP_SSL_CONF._1, config)
    spark.sparkContext.addFile(sslPair.jks.getLocation)
    if (sslPair.jks.getLocation != sslPair.jts.getLocation) {
      spark.sparkContext.addFile(sslPair.jts.getLocation)
    }
  }

  def enableSSL(spark: SparkSession, conf: H2OConf): Unit = enableSSL(spark, conf.sparkConf)

  def enableSSL(spark: SparkSession): Unit = enableSSL(spark, spark.sparkContext.conf)

  def enableFlowSSL(spark: SparkSession, conf: H2OConf): H2OConf = {
    val sslPair = SparklingWaterSecurityUtils.generateSSLPair(namePrefix = "h2o-internal-auto-flow-ssl")
    conf.setJks(sslPair.jks.getLocation)
    conf.setJksPass(sslPair.jks.pass)
  }

  def enableSSL(sc: SparkContext, conf: SparkConf): Unit = {
    logWarning("Method Security.enableSSL with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
      enableSSL(SparkSession.builder().sparkContext(sc).getOrCreate(), conf)
  }

  def enableSSL(sc: SparkContext): Unit = {
    logWarning("Method Security.enableSSL with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
    enableSSL(SparkSession.builder().sparkContext(sc).getOrCreate(), sc.conf)
  }

  def enableSSL(sc: SparkContext, conf: H2OConf): Unit = {
    logWarning("Method Security.enableSSL with an argument of type SparkContext is deprecated and " +
      "parameter of type SparkSession is preferred.")
    enableSSL(SparkSession.builder().sparkContext(sc).getOrCreate(), conf.sparkConf)
  }


}
