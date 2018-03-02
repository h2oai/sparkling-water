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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.{ElementTrackingStore, LiveEntity}

/**
  * Listener processing Sparkling Water events
  */
class AppStatusListener(conf: SparkConf, store: ElementTrackingStore, live: Boolean) extends SparkListener with Logging {

  private def onSparklingWaterStart(event: SparkListenerH2OStart): Unit = {
    val SparkListenerH2OStart(h2oCloudInfo, h2oBuildInfo, swProperties) = event
    val now = System.nanoTime()
    new SparklingWaterInfo(h2oCloudInfo, h2oBuildInfo, swProperties).write(store, now)
  }

  private def onSparklingWaterUpdate(event: SparkListenerH2ORuntimeUpdate): Unit = {
    val SparkListenerH2ORuntimeUpdate(cloudHealthy, timeInMillis) = event
    val now = System.nanoTime()
    new SparklingWaterUpdate(cloudHealthy, timeInMillis).write(store, now)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerH2OStart => onSparklingWaterStart(e)
    case e: SparkListenerH2ORuntimeUpdate => onSparklingWaterUpdate(e)
    case _ => // ignore
  }


  private class SparklingWaterInfo(h2oCloudInfo: H2OCloudInfo,
                                   h2oBuildInfo: H2OBuildInfo,
                                   swProperties: Array[(String, String)]) extends LiveEntity {
    override protected def doUpdate(): Any = {
      new SparklingWaterStartedInfo(h2oCloudInfo, h2oBuildInfo, swProperties)
    }
  }

  private class SparklingWaterUpdate(cloudHealthy: Boolean, timeInMillis: Long) extends LiveEntity {
    override protected def doUpdate(): Any = {
      new SparklingWaterUpdateInfo(cloudHealthy, timeInMillis)
    }
  }

}
