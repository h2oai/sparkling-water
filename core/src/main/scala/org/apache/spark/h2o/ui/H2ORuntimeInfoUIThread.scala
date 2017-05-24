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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OConf
import water.H2O

/**
  * Periodically publish info to UI
  */
class H2ORuntimeInfoUIThread(sc: SparkContext, conf : H2OConf) extends Thread{
  override def run(): Unit = {
    while(!Thread.interrupted()){
      sc.listenerBus.post(SparkListenerH2ORuntimeUpdate(H2O.CLOUD.healthy(),System.currentTimeMillis()))
      try {
        Thread.sleep(conf.uiUpdateInterval)
      }catch {
        case _: InterruptedException => Thread.currentThread.interrupt()
      }
    }
  }
}
