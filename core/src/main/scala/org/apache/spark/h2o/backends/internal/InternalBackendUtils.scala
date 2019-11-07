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

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.{ArgumentBuilder, SharedBackendConf, SharedBackendUtils}
import org.apache.spark.h2o.utils.{NodeDesc, ReflectionUtils}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * Various helper methods used in the internal backend
  */
private[internal] trait InternalBackendUtils extends SharedBackendUtils {


  /** Check Spark and H2O environment, update it if necessary and and warn about possible problems.
    *
    * This method checks the environments for generic configuration which does not depend on particular backend used
    * In order to check the configuration for specific backend, method checkAndUpdateConf on particular backend has to be
    * called.
    *
    * This method has to be called at the start of each method which override this one
    *
    * @param conf H2O Configuration to check
    * @return checked and updated configuration
    **/
  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)

    // Always wait for the local node - H2O node
    logWarning(s"Increasing 'spark.locality.wait' to value 0 (Infinitive) as we need to ensure we run on the nodes with H2O")
    conf.set("spark.locality.wait", "0")
    
    if (conf.clientIp.isEmpty) {
      conf.setClientIp(getHostname(SparkEnv.get))
    }
    conf
  }

}

object InternalBackendUtils extends InternalBackendUtils {

  def checkUnsupportedSparkOptions(unsupportedSparkOptions: Seq[(String, String)], conf: H2OConf): Unit = {
    unsupportedSparkOptions.foreach(opt => if (conf.contains(opt._1) && (opt._2 == "" || conf.get(opt._1) == opt._2)) {
      logWarning(s"Unsupported options ${opt._1} detected!")
      if (conf.isFailOnUnsupportedSparkParamEnabled) {
        logWarning(
          s"""
             |The application is going down, since the parameter ${SharedBackendConf.PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM} is true!
             |If you would like to skip the fail call, please, specify the value of the parameter to false.
        """.stripMargin)
        throw new IllegalArgumentException(s"Unsupported argument: $opt")
      }
    })
  }

  /**
    * Produce arguments for H2O node based on provided configuration and environment
    *
    * It is expected to run on the executor machine
    *
    * @return array of H2O launcher command line arguments
    */
  def getH2OWorkerArgs(conf: H2OConf): Seq[String] = {
    val ip = {
      val hostname = SharedBackendUtils.getHostname(SparkEnv.get)
      InternalBackendUtils.translateHostnameToIp(hostname)
    }

    new ArgumentBuilder()
      .add(getH2OCommonArgs(conf))
      .add(getLoginArgs(conf))
      .add("-log_level", conf.h2oNodeLogLevel)
      .add("-baseport", conf.nodeBasePort)
      .add("-log_dir", InternalBackendUtils.getH2ONodeLogDir(conf, SparkEnv.get))
      .addIf("-network", conf.nodeNetworkMask, conf.nodeNetworkMask.isDefined)
      .addIf("-ip", ip, conf.nodeNetworkMask.isEmpty)
      .addAsString(conf.nodeExtraProperties)
      .buildArgs()
  }

  def toH2OArgs(h2oArgs: Seq[String], executors: Array[NodeDesc] = Array()): Array[String] = {
    val flatFileString = toFlatFileString(executors)
    val flatFile = SharedBackendUtils.saveFlatFileAsFile(flatFileString)
    h2oArgs.toArray ++ Array("-flatfile", flatFile.getAbsolutePath)
  }

  private def getH2ONodeLogDir(conf: H2OConf, sparkEnv: SparkEnv): String = {
    Option(System.getProperty("spark.yarn.app.container.log.dir"))
      .map(_ + java.io.File.separator)
      .orElse(conf.h2oNodeLogDir)
      .getOrElse(SharedBackendUtils.defaultLogDir(sparkEnv.conf.getAppId))
  }

  private def translateHostnameToIp(hostname: String): String = {
    import java.net.InetAddress
    InetAddress.getByName(hostname).getHostAddress
  }

  private def toFlatFileString(executors: Array[NodeDesc]): String = {
    executors.map {
      en => s"${translateHostnameToIp(en.hostname)}:${en.port}"
    }.mkString("\n")
  }

  private[spark] def guessTotalExecutorSize(sc: SparkContext): Option[Int] = {
    sc.conf.getOption("spark.executor.instances")
      .map(_.toInt)
      .orElse(getCommandArg("--num-executors").map(_.toInt))
      .orElse({
        val sb = sc.schedulerBackend

        val num = sb match {
          case _: LocalSchedulerBackend => Some(1)
          // Use text reference to yarn backend to avoid having dependency on Spark's Yarn module
          case b if b.getClass.getSimpleName == "YarnSchedulerBackend" => Some(ReflectionUtils.reflector(b).getV[Int]("totalExpectedExecutors"))
          //case b: CoarseGrainedSchedulerBackend => b.numExistingExecutors
          case _ => None
        }
        num
      })
  }

  private def getCommandArg(argName: String): Option[String] = {
    val cmdLine = System.getProperty("sun.java.command", "").split(" ").map(_.trim)
    val valueIdx = (for (i <- cmdLine.indices; if cmdLine(i).equals(argName)) yield i + 1).headOption
    valueIdx.filter(i => i < cmdLine.length).map(i => cmdLine(i))
  }
}
