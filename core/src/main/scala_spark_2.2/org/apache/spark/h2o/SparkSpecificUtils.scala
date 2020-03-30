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

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext
import org.apache.spark.h2o.ui.{SparklingWaterListener, SparklingWaterUITab}
import org.apache.spark.ui.{SparkUITab, UIUtils}

import scala.xml.Node

object SparkSpecificUtils extends CrossSparkUtils {
  override def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: String): Seq[Node] = {
    UIUtils.headerSparkPage("Sparkling Water", content, activeTab, helpText = Some(helpText))
  }

  override def addSparklingWaterTab(sc: SparkContext): Unit = {
    val sparklingWaterListener = new SparklingWaterListener(sc.conf)
    sc.addSparkListener(sparklingWaterListener)
    new SparklingWaterUITab(sparklingWaterListener, sc.ui.get)
  }
}
