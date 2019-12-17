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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.h2o.backends.SharedBackendUtils
import org.apache.spark.h2o.utils.NodeDesc
import water.api.RestAPIManager
import water.{ExternalFrameUtils, H2O, H2OStarter, Paxos}

private[backends] trait ExternalBackendUtils extends SharedBackendUtils {

  def prepareExpectedTypes(classes: Array[Class[_]]): Array[Byte] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ExternalFrameUtils.EXPECTED_BOOL
      } else if (clazz == classOf[java.lang.Byte]) {
        ExternalFrameUtils.EXPECTED_BYTE
      } else if (clazz == classOf[java.lang.Short]) {
        ExternalFrameUtils.EXPECTED_SHORT
      } else if (clazz == classOf[java.lang.Character]) {
        ExternalFrameUtils.EXPECTED_CHAR
      } else if (clazz == classOf[java.lang.Integer]) {
        ExternalFrameUtils.EXPECTED_INT
      } else if (clazz == classOf[java.lang.Long]) {
        ExternalFrameUtils.EXPECTED_LONG
      } else if (clazz == classOf[java.lang.Float]) {
        ExternalFrameUtils.EXPECTED_FLOAT
      } else if (clazz == classOf[java.lang.Double]) {
        ExternalFrameUtils.EXPECTED_DOUBLE
      } else if (clazz == classOf[java.lang.String]) {
        ExternalFrameUtils.EXPECTED_STRING
      } else if (clazz == classOf[java.sql.Timestamp]) {
        ExternalFrameUtils.EXPECTED_TIMESTAMP
      } else if (clazz == classOf[org.apache.spark.ml.linalg.Vector]) {
        ExternalFrameUtils.EXPECTED_VECTOR
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }


  protected[backends] def startH2OClient(hc: H2OContext, nodes: Array[NodeDesc]): Unit = {
    val conf = hc.getConf
    val args = getH2OClientArgs(conf).toArray
    val launcherArgs = toH2OArgs(args, nodes)
    logDebug(s"Arguments used for launching the H2O client node: ${launcherArgs.mkString(" ")}")

    H2OStarter.start(launcherArgs, false)

    val expectedSize = conf.clusterSize.get.toInt
    val discoveredSize = waitForCloudSize(expectedSize, conf.cloudTimeout)
    if (discoveredSize < expectedSize) {
      if (conf.isAutoClusterStartUsed) {
        logError(s"Exiting! External H2O cluster was of size $discoveredSize but expected was $expectedSize!!")
        hc.stop(stopSparkContext = true)
      }
      throw new RuntimeException("Cloud size " + discoveredSize + " under " + expectedSize);
    }

    RestAPIManager(hc).registerAll()
    H2O.startServingRestApi()
  }

  protected[backends] def verifyH2OClientCloudUp(conf: H2OConf, nodes: Array[NodeDesc]): Unit = {
    val cloudMembers = H2O.CLOUD.members()
    if (cloudMembers.isEmpty) {
      if (conf.isManualClusterStartUsed) {
        throw new H2OClusterNotRunning(
          s"""
             |External H2O cluster is not running or could not be connected to. Provided configuration:
             |  cluster name            : ${conf.cloudName.get}
             |  cluster representative  : ${conf.h2oCluster.getOrElse("Using multi-cast discovery!")}
             |  cluster start timeout   : ${conf.clusterStartTimeout} sec
             |
             |It is possible that in case you provided only the cluster name, h2o is not able to cloud up
             |because multi-cast communication is limited in your network. In that case, please consider starting the
             |external H2O cluster with flatfile and set the following configuration '${ExternalBackendConf.PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1}'
        """.stripMargin)
      } else {
        throw new H2OClusterNotRunning("Problem with connecting to external H2O cluster started on yarn." +
          "Please check the YARN logs.")
      }
    }
    if (cloudMembers.size != nodes.length) {
      throw new RuntimeException("Invalid number of discovered H2O nodes.")
    }
  }

  protected[backends] def launchShellCommand(cmdToLaunch: Seq[String]): Int = {
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
