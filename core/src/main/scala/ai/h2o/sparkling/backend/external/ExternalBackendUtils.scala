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

package ai.h2o.sparkling.backend.external

import java.net.{InetAddress, NetworkInterface}

import ai.h2o.sparkling.backend.shared.SharedBackendUtils
import ai.h2o.sparkling.extensions.serde.ChunkSerdeConstants
import org.apache.spark.SparkEnv
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.h2o.utils.NodeDesc
import water.api.RestAPIManager
import water.init.{HostnameGuesser, NetworkBridge}
import water.{H2O, H2OStarter, Paxos}

private[backend] trait ExternalBackendUtils extends SharedBackendUtils {

  def prepareExpectedTypes(classes: Array[Class[_]]): Array[Byte] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ChunkSerdeConstants.EXPECTED_BOOL
      } else if (clazz == classOf[java.lang.Byte]) {
        ChunkSerdeConstants.EXPECTED_BYTE
      } else if (clazz == classOf[java.lang.Short]) {
        ChunkSerdeConstants.EXPECTED_SHORT
      } else if (clazz == classOf[java.lang.Character]) {
        ChunkSerdeConstants.EXPECTED_CHAR
      } else if (clazz == classOf[java.lang.Integer]) {
        ChunkSerdeConstants.EXPECTED_INT
      } else if (clazz == classOf[java.lang.Long]) {
        ChunkSerdeConstants.EXPECTED_LONG
      } else if (clazz == classOf[java.lang.Float]) {
        ChunkSerdeConstants.EXPECTED_FLOAT
      } else if (clazz == classOf[java.lang.Double]) {
        ChunkSerdeConstants.EXPECTED_DOUBLE
      } else if (clazz == classOf[java.lang.String]) {
        ChunkSerdeConstants.EXPECTED_STRING
      } else if (clazz == classOf[java.sql.Timestamp]) {
        ChunkSerdeConstants.EXPECTED_TIMESTAMP
      } else if (clazz == classOf[org.apache.spark.ml.linalg.Vector]) {
        ChunkSerdeConstants.EXPECTED_VECTOR
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }

  protected[backend] def startH2OClient(hc: H2OContext, conf: H2OConf, nodes: Array[NodeDesc]): Unit = {
    setClientIp(conf)
    val args = getH2OClientArgs(conf).toArray
    val launcherArgs = toH2OArgs(args, nodes)
    logDebug(s"Arguments used for launching the H2O client node: ${launcherArgs.mkString(" ")}")

    H2OStarter.start(launcherArgs, false)

    if (conf.isAutoClusterStartUsed) {
      val expectedSize = conf.clusterSize.get.toInt
      val discoveredSize = waitForCloudSize(expectedSize, conf.cloudTimeout)
      if (discoveredSize < expectedSize) {
        logError(s"Exiting! External H2O cluster was of size $discoveredSize but expected was $expectedSize!!")
        hc.stop(stopSparkContext = true)
        throw new RuntimeException("Cloud size " + discoveredSize + " under " + expectedSize);
      }
    }

    RestAPIManager(hc).registerAll()
    H2O.startServingRestApi()
  }

  protected[backend] def launchShellCommand(cmdToLaunch: Seq[String]): Int = {
    import scala.sys.process._
    val processOut = new StringBuffer()
    val processErr = new StringBuffer()

    val proc = cmdToLaunch.mkString(" ").!(ProcessLogger(
      { msg =>
        processOut.append(msg + "\n")
        println(msg)
      }, {
        errMsg =>
          processErr.append(errMsg + "\n")
          println(errMsg)
      }))

    logInfo(processOut.toString)
    logError(processErr.toString)
    proc
  }

  private def identifyClientIp(remoteAddress: String): Option[String] = {
    val interfaces = NetworkInterface.getNetworkInterfaces
    while (interfaces.hasMoreElements) {
      val interface = interfaces.nextElement()
      import scala.collection.JavaConverters._
      interface.getInterfaceAddresses.asScala.foreach { address =>
        val ip = address.getAddress.getHostAddress + "/" + address.getNetworkPrefixLength
        val cidr = HostnameGuesser.CIDRBlock.parse(ip)
        if (cidr != null && NetworkBridge.isInetAddressOnNetwork(cidr, InetAddress.getByName(remoteAddress))) {
          return Some(address.getAddress.getHostAddress)
        }
      }
    }
    None
  }

  private def setClientIp(conf: H2OConf): Unit = {
    val clientIp = identifyClientIp(conf.h2oClusterHost.get)
    if (clientIp.isDefined && conf.clientIp.isEmpty && conf.clientNetworkMask.isEmpty) {
      conf.setClientIp(clientIp.get)
    }

    if (conf.clientIp.isEmpty) {
      conf.setClientIp(ExternalH2OBackend.getHostname(SparkEnv.get))
    }
  }

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

}
