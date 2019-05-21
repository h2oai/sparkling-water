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

package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.backends.{SharedBackendUtils, SparklingBackend}
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}
import org.apache.spark.{SparkConf, SparkEnv}
import water.api.RestAPIManager
import water.{H2O, H2OStarter}


class InternalH2OBackend(@transient val hc: H2OContext) extends SparklingBackend with InternalBackendUtils with Logging {

  override def backendUIInfo: Seq[(String, String)] = Seq()

  override def stop(stopSparkContext: Boolean): Unit = {
    if (stopSparkContext) hc.sparkContext.stop()
    H2O.orderlyShutdown(5000)
    // Stop h2o when running standalone pysparkling scripts, only in client deploy mode
    //, so the user does not need explicitly close h2o.
    // In driver mode the application would call exit which is handled by Spark AM as failure
    if (hc.sparkContext.conf.get("spark.submit.deployMode", "client") != "cluster") {
      H2O.exit(0)
    }
  }

  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)
    // Note: updating Spark Conf is useless at this time in more of the cases since SparkContext is already running

    // If 'spark.executor.instances' is specified update H2O property as well
    conf.getOption("spark.executor.instances").foreach(v => conf.set("spark.ext.h2o.cluster.size", v))

    if (!conf.contains("spark.scheduler.minRegisteredResourcesRatio") && !hc.sparkContext.isLocal) {
      logWarning("The property 'spark.scheduler.minRegisteredResourcesRatio' is not specified!\n" +
        "We recommend to pass `--conf spark.scheduler.minRegisteredResourcesRatio=1`")
      // Setup the property but at this point it does not make good sense
      conf.set("spark.scheduler.minRegisteredResourcesRatio", "1")
    }


    // Setup properties for H2O configuration
    if (conf.cloudName.isEmpty) {
      conf.setCloudName("sparkling-water-" + System.getProperty("user.name", "cluster") + "_" + conf.sparkConf.getAppId)
    }

    checkUnsupportedSparkOptions(InternalH2OBackend.UNSUPPORTED_SPARK_OPTIONS, conf)
    conf
  }

  /** Initialize Sparkling H2O and start H2O cloud. */
  override def init(): Array[NodeDesc] = {
    logInfo(s"Starting H2O services: " + hc.getConf)

    val endpoints = InternalH2OBackend.registerEndpoints(hc)
    val nodes = InternalH2OBackend.startH2OWorkers(endpoints, hc._conf, hc.sparkContext.getConf, hc.sparkContext.isLocal)
    val clientNode = InternalH2OBackend.startH2OClient(hc.sparkContext.isLocal, hc._conf, nodes)
    InternalH2OBackend.distributeFlatFile(endpoints, nodes, clientNode)

    InternalH2OBackend.registerNewExecutorListener(hc)

    H2O.waitForCloudSize(endpoints.length, hc.getConf.cloudTimeout)

    // Register web API for client
    RestAPIManager(hc).registerAll()
    H2O.startServingRestApi()
    nodes.map(a => NodeDesc("", a._1, a._2))
  }

  override def epilog = ""
}

object InternalH2OBackend extends Logging {

  val UNSUPPORTED_SPARK_OPTIONS: Seq[(String, String)] = Seq(
    ("spark.dynamicAllocation.enabled", "true"),
    ("spark.speculation", "true"))

  private def registerNewExecutorListener(hc: H2OContext): Unit ={
    if (!(hc.sparkContext.isLocal || hc.sparkContext.master.startsWith("local-cluster[")) && hc.getConf.isClusterTopologyListenerEnabled) {
      hc.sparkContext.addSparkListener(new SparkListener {
        override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
          log.warn("New spark executor joined the cloud, however it won't be used for the H2O computations.")
        }
      })
    }
  }

  private def registerEndpoints(hc: H2OContext): Array[RpcEndpointRef] = {
    val endpoints = new SpreadRDDBuilder(hc, InternalBackendUtils.guessTotalExecutorSize(hc.sparkContext)).start()
    endpoints.map { ref =>
      SparkEnv.get.rpcEnv.setupEndpointRef(ref.address, ref.name)
    }
  }

  def startEndpointOnH2OWorker(conf: SparkConf): RpcEndpointRef = {
    val securityMgr = SparkEnv.get.securityManager
    val rpcEnv = RpcEnv.create("service", SharedBackendUtils.getHostname(SparkEnv.get), 0, conf, securityMgr)
    val endpoint = new H2OStarterEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("endp", endpoint)
  }

  def startH2OWorkers(endpoints: Array[RpcEndpointRef], conf: H2OConf, sparkConf: SparkConf, isLocal: Boolean): Array[(String, Int)] = {
    endpoints.map { ref =>
      val ipPort = ref.askSync[WorkerIpPort](StartH2O(conf, sparkConf, isLocal))
      (ipPort.ip, ipPort.port)
    }.distinct
  }

  def startH2OClient(isLocal: Boolean, conf: H2OConf, nodes: Array[(String, Int)]): Option[(String, Int)] = {
    if (!isLocal) {
      val h2oClientArgs = InternalBackendUtils.toH2OArgs(InternalBackendUtils.getH2OClientArgs(conf), Some(nodes.map(n => s"${n._1}:${n._2}").mkString("\n")))
      logInfo(s"Starting H2O client on the Spark Driver (${SharedBackendUtils.getHostname(SparkEnv.get)}): ${h2oClientArgs.mkString(" ")}")
      H2OStarter.start(h2oClientArgs, false)
      Some(H2O.SELF_ADDRESS.getHostAddress, H2O.API_PORT)
    } else {
      None
    }
  }

  def distributeFlatFile(endpoints: Array[RpcEndpointRef], nodes: Array[(String, Int)], clientNode: Option[(String, Int)]): Unit = {
    val fullList = if (clientNode.isDefined) {
      nodes ++ Array(clientNode.get)
    } else {
      nodes
    }
    endpoints.foreach { ref =>
      ref.send(DistributeNodesInfo(fullList))
    }
  }


  def startH2OWorkerNode(conf: H2OConf, sparkConf: SparkConf, isLocal: Boolean): Unit = {
    val logDir = identifyLogDir(conf, SparkEnv.get)
    val ip = SharedBackendUtils.getHostname(SparkEnv.get)

    val launcherArgs = InternalBackendUtils.toH2OArgs(
      InternalBackendUtils.getH2ONodeArgs(conf, isLocal)
        ++ conf.nodeNetworkMask.map(mask => Array("-network", mask)).getOrElse(Array("-ip", ip))
        ++ Array("-log_dir", logDir), Some(""))

    // Finalize REST API only if running in non-local mode.
    // In local mode, we are not going to create H2O client
    // but use executor's H2O instance directly.
    H2OStarter.start(launcherArgs, !isLocal)
  }

  private def identifyLogDir(conf: H2OConf, sparkEnv: SparkEnv): String = {
    val s = System.getProperty("spark.yarn.app.container.log.dir")
    if (s != null) {
      return s + java.io.File.separator
    }
    if (conf.h2oNodeLogDir.isDefined) {
      conf.h2oNodeLogDir.get
    } else {
      // Needs to be executed at remote node!
      SharedBackendUtils.defaultLogDir(sparkEnv.conf.getAppId)
    }
  }

}
