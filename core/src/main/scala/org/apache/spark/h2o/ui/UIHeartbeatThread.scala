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
import water.util.PrettyPrint

/**
  * Periodically publish information to Spark UI
  */
class UIHeartbeatThread(sc: SparkContext, conf: H2OConf) extends Thread {
  override def run(): Unit = {
    while (!Thread.interrupted()) {
      val nodes = H2O.CLOUD.members() ++ Array(H2O.SELF)
      val memoryInfo = nodes.map(node => (node.getIpPortString, PrettyPrint.bytes(node._heartbeat.get_free_mem())))
      sc.listenerBus.post(SparklingWaterHeartbeatEvent(H2O.CLOUD.healthy(), System.currentTimeMillis(), memoryInfo))
      try {
        Thread.sleep(conf.uiUpdateInterval)
      } catch {
        case _: InterruptedException => Thread.currentThread.interrupt()
      }
    }
  }
}
