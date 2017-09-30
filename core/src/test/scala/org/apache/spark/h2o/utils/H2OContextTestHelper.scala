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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.h2o.backends.SharedBackendConf.PROP_BACKEND_CLUSTER_MODE
import org.apache.spark.h2o.backends.external.ExternalBackendConf

object H2OContextTestHelper extends ExternalBackendManualTestStarter {

  def isManualClusterStartModeUsed(conf: Option[SparkConf] = None): Boolean = {
    if (conf.isDefined) {
      conf.get.getOption(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._1).getOrElse(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._2) == "manual"
    } else {
      sys.props.getOrElse(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._1, ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._1) ==  "manual"
    }
  }

  def isManualClusterStartModeUsed(conf: SparkConf): Boolean = isManualClusterStartModeUsed()

  def isAutoClusterStartModeUsed(conf: Option[SparkConf] = None): Boolean = !isManualClusterStartModeUsed(conf)
  def isAutoClusterStartModeUsed(): Boolean = !isManualClusterStartModeUsed()

  def isExternalClusterUsed(conf: Option[SparkConf] = None): Boolean = {
    if (conf.isDefined) {
      conf.get.getOption(PROP_BACKEND_CLUSTER_MODE._1).getOrElse(PROP_BACKEND_CLUSTER_MODE._2) == "external"
    } else {
      sys.props.getOrElse(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2) == "external"
    }
  }

  def isExternalClusterUsed(conf: SparkConf): Boolean = isExternalClusterUsed(Option(conf))


  def createH2OContext(sc: SparkContext, numH2ONodes: Int): H2OContext = {
    val h2oConf = new H2OConf(sc)
    if (isExternalClusterUsed(sc.getConf) && isManualClusterStartModeUsed(sc.getConf)) {
      startExternalH2OCloud(numH2ONodes, sc.getConf)
    }
    H2OContext.getOrCreate(sc, h2oConf.setNumOfExternalH2ONodes(numH2ONodes))
  }

  def stopH2OContext(sc: SparkContext, hc: H2OContext): Unit = {
    if (isExternalClusterUsed(sc.getConf) && isManualClusterStartModeUsed(sc.getConf)) {
      stopExternalH2OCloud()
    }
    hc.stop()
  }
}
