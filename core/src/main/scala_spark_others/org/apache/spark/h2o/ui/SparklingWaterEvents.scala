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

import org.apache.spark.scheduler.SparkListenerEvent

/**
  * Event representing start of H2OContext
  */
case class SparkListenerH2OStart(h2oCloudInfo: H2OCloudInfo,
                                 h2oBuildInfo: H2OBuildInfo,
                                 swProperties: Array[(String, String)]) extends SparkListenerEvent


/**
  * Event representing update of H2O status at run-time
  */
case class SparkListenerH2ORuntimeUpdate(cloudHealthy: Boolean, timeInMillis: Long, memoryInfo: Array[(String, String)]) extends SparkListenerEvent
