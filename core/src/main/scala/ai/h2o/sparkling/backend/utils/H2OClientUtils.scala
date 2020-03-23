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

package ai.h2o.sparkling.backend.utils

import java.net.{InetAddress, NetworkInterface}

import ai.h2o.sparkling.backend.NodeDesc
import ai.h2o.sparkling.backend.api.RestAPIManager
import org.apache.spark.SparkEnv
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.init.HostnameGuesser
import water.{H2O, H2OStarter, Paxos}

/**
 * All helper methods which are used when H2O client is running on Spark driver
 * This class should be removed after we remove H2O client from Scala as well
 */
object H2OClientUtils extends SharedBackendUtils {

  val PROP_REST_API_BASED_CLIENT: (String, Boolean) = ("spark.ext.h2o.rest.api.based.client", false)

  def isH2OClientBased(conf: H2OConf): Boolean = {
    !conf.getBoolean(PROP_REST_API_BASED_CLIENT._1, PROP_REST_API_BASED_CLIENT._2)
  }

  /**
   * Get common arguments for H2O client.
   *
   * @return array of H2O client arguments.
   */
  private def getH2OClientArgs(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add(getH2OWorkerAsClientArgs(conf))
      .add("-network", conf.clientNetworkMask)
      .addIf("-ip", conf.clientIp, conf.clientNetworkMask.isEmpty)
      .add("-client")
      .buildArgs()
  }

  def getH2OCommonArgsWhenClientBased(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add("-allow_clients")
      .add("-client_disconnect_timeout", conf.clientCheckRetryTimeout)
      .buildArgs()
  }

  def getExtraExternalBackendArgsWhenClientBased(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add("-sw_ext_backend")
      .add(Seq("-J", "-client_disconnect_timeout", "-J", conf.clientCheckRetryTimeout.toString))
      .buildArgs()
  }

  def startH2OClient(hc: H2OContext, conf: H2OConf, nodes: Array[NodeDesc]): NodeDesc = {
    if (conf.runsInExternalClusterMode) {
      setClientIp(conf)
    } else {
      if (conf.clientIp.isEmpty) {
        conf.setClientIp(getHostname(SparkEnv.get))
      }
    }
    if (!(conf.runsInInternalClusterMode && hc.sparkContext.isLocal)) {
      val args = getH2OClientArgs(conf).toArray
      val launcherArgs = toH2OArgs(args, nodes)
      logDebug(s"Arguments used for launching the H2O client node: ${launcherArgs.mkString(" ")}")

      H2OStarter.start(launcherArgs, false)

      if (conf.runsInInternalClusterMode || conf.isAutoClusterStartUsed) {
        val expectedSize = nodes.length
        val discoveredSize = waitForCloudSize(expectedSize, conf.cloudTimeout)
        if (discoveredSize < expectedSize) {
          logError(s"Exiting! H2O cluster was of size $discoveredSize but expected was $expectedSize!!")
          hc.stop(stopSparkContext = true)
          throw new RuntimeException("Cloud size " + discoveredSize + " under " + expectedSize);
        }
      }
      RestAPIManager(hc).registerAll()
      H2O.startServingRestApi()
    }
    NodeDesc(SparkEnv.get.executorId, H2O.SELF_ADDRESS.getHostAddress, H2O.API_PORT)
  }

  /**
   * Wait for cloud size from the H2O client point of view
   */
  private def waitForCloudSize(expectedSize: Int, timeoutInMilliseconds: Long): Int = {
    val start = System.currentTimeMillis()
    while (System.currentTimeMillis() - start < timeoutInMilliseconds) {
      if (H2O.CLOUD.size() >= expectedSize && Paxos._commonKnowledge) {
        return H2O.CLOUD.size()
      }
      try {
        Thread.sleep(100)
      } catch {
        case _: InterruptedException =>
      }
    }
    H2O.CLOUD.size()
  }

  private def setClientIp(conf: H2OConf): Unit = {
    val clientIp = identifyClientIp(conf.h2oClusterHost.get)
    if (clientIp.isDefined && conf.clientNetworkMask.isEmpty) {
      conf.setClientIp(clientIp.get)
    }

    if (conf.clientIp.isEmpty) {
      conf.setClientIp(getHostname(SparkEnv.get))
    }
  }

  private def identifyClientIp(remoteAddress: String): Option[String] = {
    val interfaces = NetworkInterface.getNetworkInterfaces
    while (interfaces.hasMoreElements) {
      val interface = interfaces.nextElement()
      import scala.collection.JavaConverters._
      interface.getInterfaceAddresses.asScala.foreach { address =>
        val ip = address.getAddress.getHostAddress + "/" + address.getNetworkPrefixLength
        val cidr = HostnameGuesser.CIDRBlock.parse(ip)
        if (cidr != null && cidr.isInetAddressOnNetwork(InetAddress.getByName(remoteAddress))) {
          return Some(address.getAddress.getHostAddress)
        }
      }
    }
    None
  }
}
