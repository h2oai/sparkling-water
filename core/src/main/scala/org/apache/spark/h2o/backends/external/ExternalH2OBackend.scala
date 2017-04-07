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


import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.spark.h2o.backends.SparklingBackend
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext, H2OLogging}
import water.api.RestAPIManager
import water.{H2O, H2OStarter, UDPClientEvent}

import scala.io.Source
import scala.util.Random
import scala.util.control.NoStackTrace


class ExternalH2OBackend(val hc: H2OContext) extends SparklingBackend with ExternalBackendUtils with H2OLogging {

  private var yarnAppId: Option[String] = None
  private var externalIP: Option[String] = None

  def launchH2OOnYarn(sparkAppId: String, conf: H2OConf): String = {
    import ExternalH2OBackend._

    var cmdToLaunch = Seq[String]("hadoop",
      "jar", conf.h2oDriverPath.get)

    conf.sslConf match {
      case Some(ssl) =>
        val sslConfig = new Properties()
        sslConfig.load(new FileInputStream(ssl))
        cmdToLaunch = cmdToLaunch ++ Array("-files", sslConfig.get("h2o_ssl_jks_internal") + "," + sslConfig.get("h2o_ssl_jts"))
        cmdToLaunch = cmdToLaunch ++ Array("-internal_security", ssl)
        logInfo(s"Running external cluster in encrypted mode with config $ssl")
      case _ =>
    }
    // Application tags shown in Yarn Resource Manager UI
    val yarnAppTags = s"${TAG_EXTERNAL_H2O},${TAG_SPARK_APP.format(sparkAppId)}"
    
    cmdToLaunch = cmdToLaunch ++ Seq[String](
      conf.YARNQueue.map("-Dmapreduce.job.queuename=" + _ ).getOrElse(""),
      s"-Dmapreduce.job.tags=${yarnAppTags}",
      "-nodes", conf.numOfExternalH2ONodes.get,
      "-notify", conf.clusterInfoFile.get,
      "-J", "-md5skip",
      "-jobname", H2O_JOB_NAME.format(sparkAppId),
      "-mapperXmx", conf.mapperXmx,
      "-output", conf.HDFSOutputDir.get,
      "-J", "-log_level", "-J", conf.h2oNodeLogLevel,
      "-disown"
    )

    // start external h2o cluster and log the output
    logInfo("Command used to start H2O on yarn: " + cmdToLaunch.mkString(" "))

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


    if(!new File(hc.getConf.clusterInfoFile.get).exists()){
      throw new RuntimeException(
            "Cluster notification file wasn't created. The possible causes are: \n" +
            "   1) The timeout for clouding up is too small and H2O didn't cloud up. \n" +
                "In that case please try to increase the timeout for starting the external as: \n" +
                "Python: H2OConf(sc).set_cluster_start_timeout(timeout).... \n" +
                "Scala:  new H2OConf(sc).setClusterStartTimeout(timeout).... \n" +
            "   2) The file couldn't be created because of missing write rights."
      )
    }
    // get ip port
    val clusterInfo = Source.fromFile(hc.getConf.clusterInfoFile.get).getLines
    val ipPort = clusterInfo.next()
    yarnAppId = Some(clusterInfo.next().replace("job", "application"))
    externalIP = Some(ipPort)

    logInfo(s"Yarn ID obtained from cluster file: $yarnAppId")
    logInfo(s"Cluster ip and port obtained from cluster file: $ipPort")

    sys.ShutdownHookThread {
      if(hc.getConf.isAutoClusterStartUsed){
        stopExternalCluster()
        deleteYarnFiles()
      }
    }
    assert(proc == 0, s"Starting external H2O cluster failed with return value $proc.")
    ipPort
  }

  private def deleteYarnFiles(): Unit = {
    try {
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hc.sparkContext.hadoopConfiguration)
      hdfs.delete(new Path(hc.getConf.HDFSOutputDir.get), true)
    }catch {
      case e: Exception =>
        logError(s"Error when deleting HDFS output dir at ${hc.getConf.HDFSOutputDir.get}" +
          s", original message: ${e.getMessage}")
    }
    try {
      new File(hc.getConf.clusterInfoFile.get).delete()
    } catch {
      case e: Exception =>
        logError(s"Error when deleting cluster info file at ${hc.getConf.clusterInfoFile.get}" +
          s", original message: ${e.getMessage}")
    }
  }

  private def stopExternalCluster(): Unit = {
    // Send disconnect command from the watchdog client in case of orderly shutdown
    UDPClientEvent.ClientEvent.Type.DISCONNECT.broadcast(H2O.SELF)
    log.info("Stopping external H2O cluster!")
  }

  override def init(): Array[NodeDesc] = {
    if (hc.getConf.isAutoClusterStartUsed) {
      // start h2o instances on yarn
      logInfo("Starting H2O cluster on YARN")
      val ipPort = launchH2OOnYarn(hc.sparkContext.applicationId, hc.getConf)
      hc._conf.setH2OCluster(ipPort)
    }
    // Start H2O in client mode and connect to existing H2O Cluster
    logTrace("Starting H2O on client mode and connecting it to existing h2o cluster")

    val h2oClientArgs = if(hc.getConf.isAutoClusterStartUsed){
      getH2OClientArgs(hc.getConf) ++ Array("-watchdog_client")
    }else{
      getH2OClientArgs(hc.getConf)
    }
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

  override def backendUIInfo: Seq[(String,String)] = {
    Seq(
      ("External backend YARN AppID", yarnAppId),
      ("External IP", externalIP)
    ).filter(_._2.nonEmpty).map{ case (k,v) => (k, v.get)}
  }

  override def stop(stopSparkContext: Boolean): Unit = {
    // stop only external h2o cluster running on yarn
    // otherwise stopping is not supported
    if(hc.getConf.isAutoClusterStartUsed){
      deleteYarnFiles()
      stopExternalCluster()
    }else {
      if (stopSparkContext) {
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

object ExternalH2OBackend {
  // This string tags instances of H2O launched from Sparkling Water
  val TAG_EXTERNAL_H2O = "H2O/Sparkling-Water"
  // Another tag which identifies launcher - aka Spark application
  val TAG_SPARK_APP = "Sparkling-Water/Spark/%s"
  // Job name for H2O Yarn job
  val H2O_JOB_NAME = "H2O_via_SparklingWater_%s"
}

class H2OClusterNotRunning(msg: String) extends Exception(msg) with NoStackTrace
