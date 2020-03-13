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

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.utils.{ArgumentBuilder, SharedBackendUtils}
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.{SparkContext, SparkEnv}

private[backends] trait InternalBackendUtils extends SharedBackendUtils {

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
      val hostname = getHostname(SparkEnv.get)
      translateHostnameToIp(hostname)
    }

    new ArgumentBuilder()
      .add(getH2OCommonArgs(conf))
      .add(getH2OSecurityArgs(conf))
      .add("-log_level", conf.h2oNodeLogLevel)
      .add("-baseport", conf.nodeBasePort)
      .add("-log_dir", getH2ONodeLogDir(conf, SparkEnv.get))
      .addIf("-network", conf.nodeNetworkMask, conf.nodeNetworkMask.isDefined)
      .addIf("-ip", ip, conf.nodeNetworkMask.isEmpty)
      .addAsString(conf.nodeExtraProperties)
      .buildArgs()
  }

  private def getH2ONodeLogDir(conf: H2OConf, sparkEnv: SparkEnv): String = {
    Option(System.getProperty("spark.yarn.app.container.log.dir"))
      .map(_ + java.io.File.separator)
      .orElse(conf.h2oNodeLogDir)
      .getOrElse(defaultLogDir(sparkEnv.conf.getAppId))
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
