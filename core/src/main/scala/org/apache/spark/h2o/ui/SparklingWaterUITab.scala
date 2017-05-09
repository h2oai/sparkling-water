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

import org.apache.spark.ui.{SparkUI, SparkUITab}


/**
  * Enrich Spark UI by a Sparkling Water specific tab.
  */
private[h2o] class SparklingWaterUITab(val listener: SparklingWaterListener, parent: SparkUI)
  extends SparkUITab(parent, "sparkling-water") {

  override val name = "Sparkling Water \u2728"

  attachPage(SparklingWaterInfoPage(this))
  parent.attachTab(this)

  def getSparkUser: String = parent.getSparkUser
}
