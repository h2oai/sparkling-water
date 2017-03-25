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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

import org.apache.spark.h2o.backends.SparklingBackend
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.listeners.ExecutorAddNotSupportedListener
import water.api.RestAPIManager
import water.{H2O, H2OStarter}

import scala.util.Random


class InternalH2OBackend(@transient val hc: H2OContext) extends SparklingBackend with InternalBackendUtils with Logging {

  override def backendUIInfo: Seq[(String, String)] = Seq()

  override def stop(stopSparkContext: Boolean): Unit = {
    if (stopSparkContext) hc.sparkContext.stop()
    H2O.orderlyShutdown(5000)
    H2O.exit(0)
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
      conf.setCloudName("sparkling-water-" + System.getProperty("user.name", "cluster") + "_" + Math.abs(Random.nextInt()))
    }

    checkUnsupportedSparkOptions(InternalH2OBackend.UNSUPPORTED_SPARK_OPTIONS, conf)
    conf
  }

  /** Initialize Sparkling H2O and start H2O cloud. */
  override def init(): Array[NodeDesc] = {

    logInfo(s"Starting H2O services: " + hc.getConf)
    // Create dummy RDD distributed over executors
    val (spreadRDD, spreadRDDNodes) = new SpreadRDDBuilder(hc, InternalBackendUtils.guessTotalExecutorSize(hc.sparkContext)).build()

    if(hc.getConf.isClusterTopologyListenerEnabled){
      // Attach listener which kills H2O cluster when new Spark executor has been launched ( which means
      // that this executors hasn't been discovered during the spreadRDD phase)
      hc.sparkContext.addSparkListener(new ExecutorAddNotSupportedListener())
    }

    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = spreadRDDNodes.map(_.nodeId).distinct
    val executorIds = allExecutorIds
    // The collected executors based on IDs should match
    assert(spreadRDDNodes.length == executorIds.length,
      s"Unexpected number of executors ${spreadRDDNodes.length}!=${executorIds.length}")
    // H2O is executed only on the subset of Spark cluster - fail
    if (executorIds.length < allExecutorIds.length) {
      throw new IllegalArgumentException(s"""Spark cluster contains ${allExecutorIds.length},
               but H2O is running only on ${executorIds.length} nodes!""")
    }
    // Execute H2O on given nodes
    logInfo(s"""Launching H2O on following ${spreadRDDNodes.length} nodes: ${spreadRDDNodes.mkString(",")}""")

    var h2oNodeArgs = InternalBackendUtils.getH2ONodeArgs(hc.getConf)
    // Disable web on h2o nodes in non-local mode
    if(!hc.sparkContext.isLocal){
      h2oNodeArgs = h2oNodeArgs ++ Array("-disable_web")
    }else{
      // In local mode we don't start h2o client and use standalone h2o mode right away. We need to set login configuration
      // in this case explicitly
      h2oNodeArgs = h2oNodeArgs ++ getLoginArgs(hc.getConf)
    }
    logDebug(s"Arguments used for launching h2o nodes: ${h2oNodeArgs.mkString(" ")}")
    val executors = InternalBackendUtils.startH2O(hc.sparkContext, spreadRDD, spreadRDDNodes.length, h2oNodeArgs, hc.getConf.nodeNetworkMask)

    // Connect to a cluster via H2O client, but only in non-local case
    if (!hc.sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + executors.length)
      // Get arguments for this launch including flatfile ( Do not use IP if network mask is specified)
      val h2oClientArgs = InternalBackendUtils.toH2OArgs(InternalBackendUtils.getH2OClientArgs(hc.getConf), hc.getConf, executors)
      logInfo(s"Starting H2O client on the Spark Driver (${getHostname(SparkEnv.get)}): ${h2oClientArgs.mkString(" ")}")
      // Launch H2O
      H2OStarter.start(h2oClientArgs, false)
    }
    // And wait for right cluster size
    H2O.waitForCloudSize(executors.length, hc.getConf.cloudTimeout)

    // Register web API for client
    RestAPIManager(hc).registerAll()
    H2O.finalizeRegistration()
    executors
  }

}

object InternalH2OBackend{
  val UNSUPPORTED_SPARK_OPTIONS =  Seq(
    ("spark.dynamicAllocation.enabled", "true"),
    ("spark.speculation", "true"))
}
