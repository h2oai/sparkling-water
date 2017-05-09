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
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
  * H2OContext is started
  */
case class SparkListenerH2OStart(h2oCloudInfo: H2OCloudInfo,
                                 h2oBuildInfo: H2OBuildInfo,
                                 swProperties: Array[(String, String)]) extends SparkListenerEvent


/**
  * Update of H2O status at run-time
  */
case class SparkListenerH2ORuntimeUpdate(cloudHealthy: Boolean, timeInMillis: Long)
  extends SparkListenerEvent

/**
  * Required at the history server in order to create the [[SparklingWaterHistoryListener]]
  */
class SparklingWaterHistoryListenerFactory extends SparkHistoryListenerFactory {

  override def createListeners(conf: SparkConf, sparkUI: SparkUI): Seq[SparkListener] = {
    List(new SparklingWaterHistoryListener(conf, sparkUI))
  }
}

/**
  * Listener processing related sparkling water spark events
  */
class SparklingWaterListener(conf: SparkConf) extends SparkListener with Logging {
  var uiReady = false
  var h2oCloudInfo: Option[H2OCloudInfo] = None
  var h2oBuildInfo: Option[H2OBuildInfo] = None
  var swProperties: Option[Array[(String, String)]] = None
  var cloudHealthy = true
  var lastTimeHeadFromH2O: Long = 0
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerH2OStart(h2oCloudInfo, h2oBuildInfo, swProperties) => {
      this.h2oCloudInfo = Some(h2oCloudInfo)
      this.h2oBuildInfo = Some(h2oBuildInfo)
      this.swProperties = Some(swProperties)
      cloudHealthy = h2oCloudInfo.cloudHealthy
      lastTimeHeadFromH2O = h2oCloudInfo.h2oStartTime
      uiReady = true
    }
    case SparkListenerH2ORuntimeUpdate(cloudHealthy, timeInMillis) => {
      this.cloudHealthy = cloudHealthy
      this.lastTimeHeadFromH2O = timeInMillis
  }
    case _ => // Ignore
  }

}


/**
  * A [[SparklingWaterListener]] for rendering the Sparkling Water UI in the history server.
  */
class SparklingWaterHistoryListener(conf: SparkConf, sparkUI: SparkUI)
  extends SparklingWaterListener(conf) {

  private var sparklingWaterTabAttached = false


  override def onExecutorMetricsUpdate(u: SparkListenerExecutorMetricsUpdate): Unit = {
    // Do nothing; these events are not logged
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case _: SparkListenerH2OStart =>
      if (!sparklingWaterTabAttached) {
        new SparklingWaterUITab(this, sparkUI)
        sparklingWaterTabAttached = true
      }
      super.onOtherEvent(event)
    case _ => super.onOtherEvent(event)
  }
}


case class H2OCloudInfo(
                          localClientIpPort: String,
                          cloudHealthy: Boolean,
                          cloudNodes: Array[String],
                          extraBackendInfo: Seq[(String, String)],
                          h2oStartTime: Long)

case class H2OBuildInfo(
                         h2oBuildVersion: String,
                         h2oGitBranch: String,
                         h2oGitSha: String,
                         h2oGitDescribe: String,
                         h2oBuildBy: String,
                         h2oBuildOn: String
                       )
