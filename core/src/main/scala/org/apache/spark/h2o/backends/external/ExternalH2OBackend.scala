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


import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.h2o.backends.SparklingBackend
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.internal.Logging
import water.api.RestAPIManager
import water.{H2O, H2OStarter}

import scala.io.Source
import scala.util.Random
import scala.util.control.NoStackTrace


class ExternalH2OBackend(val hc: H2OContext) extends SparklingBackend with ExternalBackendUtils with Logging {

  private var yarnAppId: String = _
  def launchH2OOnYarn(conf: H2OConf): String = {
    val cmdToLaunch = Seq[String](
      "hadoop",
      "jar",
      conf.h2oDriverPath.get,
      conf.YARNQueue.map("-Dmapreduce.job.queuename=" + _ ).getOrElse(""),
      "-nodes", conf.numOfExternalH2ONodes.get,
      "-notify", conf.clusterInfoFile.get,
      "-J", "-md5skip",
      "-jobname", conf.cloudName.get,
      "-mapperXmx", conf.mapperXmx,
      "-output", conf.HDFSOutputDir.get,
      "-disown"
    )

    // start external h2o cluster and log the output
    logInfo("Command used to start H2O on yarn: " + cmdToLaunch.mkString)
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

    logDebug(processOut.toString)
    logDebug(processErr.toString)


    // get ip port
    val clusterInfo = Source.fromFile(hc.getConf.clusterInfoFile.get).getLines
    val ipPort = clusterInfo.next()
    yarnAppId = clusterInfo.next().replace("job", "application")

    logInfo(s"Yarn ID obtained from cluster file: $yarnAppId")
    logInfo(s"Cluster ip and port obtained from cluster file: $ipPort")

    sys.ShutdownHookThread {
      if(hc.getConf.isAutoClusterStartUsed){
        shutdownCleanUp()
      }
    }
    assert(proc == 0, s"Starting external H2O cluster failed with return value $proc.")
    ipPort
  }

  private def shutdownCleanUp(): Unit ={
    try {
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hc.sparkContext.hadoopConfiguration)
      hdfs.delete(new Path(hc.getConf.HDFSOutputDir.get), true)
      new File(hc.getConf.clusterInfoFile.get).delete()
    }catch {
      case e: Exception => log.error(e.getMessage)
    }
    import scala.sys.process._
    // kill the job
    s"yarn application -kill $yarnAppId".!
  }

  override def init(): Array[NodeDesc] = {

    if (hc.getConf.isAutoClusterStartUsed) {
      // start h2o instances on yarn
      logInfo("Starting H2O cluster on YARN")
      val ipPort = launchH2OOnYarn(hc.getConf)
      hc._conf.setH2OCluster(ipPort)
    }
    // Start H2O in client mode and connect to existing H2O Cluster
    logTrace("Starting H2O on client mode and connecting it to existing h2o cluster")
    val h2oClientArgs = getH2OClientArgs(hc.getConf)
    logDebug(s"Arguments used for launching h2o client node: ${h2oClientArgs.mkString(" ")}")
    H2OStarter.start(h2oClientArgs, false)

    if (hc.getConf.numOfExternalH2ONodes.isDefined) {
      H2O.waitForCloudSize(hc.getConf.numOfExternalH2ONodes.get.toInt, hc.getConf.cloudTimeout)
    }
    // Register web API for client
    RestAPIManager(hc).registerAll()
    H2O.finalizeRegistration()

    if (cloudMembers.length == 0) {
      if (hc.getConf.h2oDriverPath.isEmpty) {
        throw new H2OClusterNotRunning(
          s"""
           |
          |H2O Cluster is not running or couldn't be connected to. Provided configuration:
           |   cloud name            : ${hc.getConf.cloudName.get}

           |   cloud representative  : ${hc.getConf.h2oCluster.getOrElse(
            "Not set, connecting to cluster in multicast mode")}

           |
          | It is possible that in case you provided only cloud name h2o is not able to cloud up because multicast
           | communication is limited in your network. In that case please consider setting "${ExternalBackendConf.
            PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1}
"
           | property and starting nodes in h2o cluster with -flatfile option describing the cluster.
        """.stripMargin)
        }else{
        throw new H2OClusterNotRunning("Problem during connection to h2o cluster started on yarn. Please check the logs")
      }
    }

    cloudMembers
  }

  override def stop(stopSparkContext: Boolean): Unit = {
    // stop only when we have external h2o cluster running on yarn
    // otherwise stopping is not supported
    if(hc.getConf.HDFSOutputDir.isDefined){
      shutdownCleanUp()

    }else{
      if (stopSparkContext){
        hc.sparkContext.stop()
      }
      H2O.orderlyShutdown(1000)
      H2O.exit(0)
    }
  }

  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)


    if(conf.isAutoClusterStartUsed) {
      lazy val driverPath = sys.env.get("H2O_EXTENDED_JAR")
      if(conf.h2oDriverPath.isEmpty && driverPath.isEmpty){
        throw new IllegalArgumentException(
          """Path to h2o driver has to be specified when using automatic cluster start either vie method available
            | on the configuration or H2O_EXTENDED_JAR property
          """.stripMargin)
      }
      if (conf.h2oDriverPath.isEmpty && driverPath.isDefined) {
        log.info(s"Obtaining path to extended h2o driver from environment variable. Specified path is ${driverPath.get}")
        conf.setH2ODriverPath(driverPath.get)
      }

      if (conf.cloudName.isEmpty) {
        conf.setCloudName("sparkling-water-" + System.getProperty("user.name", "cluster") + "_" + Math.abs(Random.nextInt()))
      }

      if (conf.numOfExternalH2ONodes.isEmpty) {
        throw new IllegalArgumentException("Number of h2o nodes has to be specified in external auto start backend mode.")
      }

      if (conf.HDFSOutputDir.isEmpty) {
        conf.setHDFSOutputDir(conf.cloudName.get)
      }

      if(conf.clusterInfoFile.isEmpty){
        conf.setClusterConfigFile("notify_" + conf.cloudName.get)
      }

    } else {
      if (conf.cloudName.isEmpty) {
        throw new IllegalArgumentException(
          """Cloud name has to be specified when using external backend cluster mode in manual start mode. It can be set either using H2OConf
            |instance or via 'spark.ext.h2o.cloud.name' spark configuration property""".stripMargin)
      }
    }
    conf
  }
}

class H2OClusterNotRunning(msg: String) extends Exception(msg) with NoStackTrace
