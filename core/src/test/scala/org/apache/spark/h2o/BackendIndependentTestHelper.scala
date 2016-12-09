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

package org.apache.spark.h2o

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.backends.SharedBackendConf._
import org.apache.spark.h2o.utils.ExternalBackendTestHelper
import org.scalatest.Suite

/**
  * Trait used to create tests which can run on both internal and external backend
  */
trait BackendIndependentTestHelper extends ExternalBackendTestHelper {
  self: Suite =>

  def testsInExternalMode(conf: Option[SparkConf] = None): Boolean = {
    if (conf.isDefined) {
      conf.get.getOption(PROP_BACKEND_CLUSTER_MODE._1).getOrElse(PROP_BACKEND_CLUSTER_MODE._2) == "external"
    } else {
      sys.props.getOrElse(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2) == "external"
    }
  }

  def testsInExternalMode(conf: SparkConf): Boolean = {
    testsInExternalMode(Option(conf))
  }

  def createH2OContext(sc: SparkContext, numH2ONodes: Int): H2OContext = {
    val h2oConf = new H2OConf(sc)
    if (testsInExternalMode(sc.getConf)) {
      startCloud(numH2ONodes, sc.getConf)
    }
    H2OContext.getOrCreate(sc, h2oConf.setNumOfExternalH2ONodes(numH2ONodes))
  }

  def stopCloudIfExternal(sc: SparkContext): Unit = {
    if (testsInExternalMode(sc.getConf)) {
      stopCloud()
    }
  }
}
