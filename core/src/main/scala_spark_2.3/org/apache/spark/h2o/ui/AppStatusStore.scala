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

package org.apache.spark.h2o.ui

import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVStore

/**
  * Sparkling Water accessors into general Spark KVStore
  */
class AppStatusStore(store: KVStore, val listener: Option[AppStatusListener] = None)
  extends SparklingWaterInfoProvider {

  private def getStartedInfo(): SparklingWaterStartedInfo = {
    val klass = classOf[SparklingWaterStartedInfo]
    store.read(klass, klass.getName)
  }

  private def getUpdateInfo(): SparklingWaterUpdateInfo = {
    val klass = classOf[SparklingWaterUpdateInfo]
    store.read(klass, klass.getName)
  }

  override def isSparklingWaterStarted(): Boolean = {
    val klass = classOf[SparklingWaterStartedInfo]
    store.count(klass) != 0
  }

  override def localIpPort: String = getStartedInfo().h2oClusterInfo.localClientIpPort

  override def sparklingWaterProperties: Seq[(String, String)] = getStartedInfo().swProperties

  override def H2OClusterInfo: H2OClusterInfo = getStartedInfo().h2oClusterInfo

  override def H2OBuildInfo: H2OBuildInfo = getStartedInfo().h2oBuildInfo

  override def memoryInfo: Array[(String, String)] = getUpdateInfo().memoryInfo

  override def timeInMillis: Long = getUpdateInfo().timeInMillis

  override def isCloudHealthy: Boolean = getUpdateInfo().cloudHealthy
}

/**
  * Object encapsulating information produced when Sparkling Water is started
  */
class SparklingWaterStartedInfo(val h2oClusterInfo: H2OClusterInfo,
                                val h2oBuildInfo: H2OBuildInfo,
                                val swProperties: Array[(String, String)]) {
  // Use lass name ad key since there is always just a single instance of this object in KVStore
  @KVIndexParam val id: String = getClass.getName
}

/**
  * Object encapsulating information about Sparkling Water Heartbeat
  */
class SparklingWaterUpdateInfo(val cloudHealthy: Boolean, val timeInMillis: Long, val memoryInfo: Array[(String, String)]) {
  // Use lass name ad key since there is always just a single instance of this object in KVStore
  @KVIndexParam val id: String = getClass.getName
}
