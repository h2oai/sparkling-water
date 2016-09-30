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

package water.messaging.driver

import java.util
import java.util.concurrent.CountDownLatch

import org.apache.spark.h2o.H2OContext
import water.H2O

import scala.annotation.meta.{field, param}

class SWDriverServiceImpl(@(transient @param @field) val hc: H2OContext,
                          @(transient @param @field) var port: Int) extends DriverService {

  final val stopLatch = new CountDownLatch(1)

  def awaitStop(): Unit = {
    stopLatch.await()
  }

  override def stop() = {
    // DON'T use hc.stop or anything that calls System.exit() -> it will call the shutdown hook
    // before YARN gets the exit status and will mark the YARN job as failed -> this might mean
    // a restart of the whole job. Thank you.
    hc.sparkContext.stop()
    H2O.orderlyShutdown(1000)
    stopLatch.countDown()
  }

  import scala.collection.JavaConverters._
  override def get(names: Array[String]): util.Map[String, AnyRef] = names.map {
    case "id" =>
      ("id", hc.sparkContext.applicationId)
    case "ip" =>
      // TODO good ip? share port with the ExternalMessageChannel!
      val ip: String = hc.h2oLocalClientIp + ":" + hc.h2oLocalClientPort
      ("ip", ip)
    case "messaging" =>
      val ip: String = hc.h2oLocalClientIp + ":" + port
      ("messaging", ip)
  }.toMap[String, AnyRef].asJava

  override def setPort(port: Int): Unit = this.port = port
}
