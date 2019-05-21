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
import org.apache.spark.h2o.backends.{SharedBackendConf, SharedBackendUtils}
import org.apache.spark.h2o.utils.{NodeDesc, ReflectionUtils}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * Various helper methods used in the internal backend
  */
private[internal] trait InternalBackendUtils extends SharedBackendUtils {

  def checkUnsupportedSparkOptions(unsupportedSparkOptions: Seq[(String, String)], conf: H2OConf): Unit = {
    unsupportedSparkOptions.foreach(opt => if (conf.contains(opt._1) && (opt._2 == None || conf.get(opt._1) == opt._2)) {
      logWarning(s"Unsupported options ${opt._1} detected!")
      if (conf.isFailOnUnsupportedSparkParamEnabled) {
        logWarning(
          s"""
             |The application is going down, since the parameter ${SharedBackendConf.PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM} is true!
             |If you would like to skip the fail call, please, specify the value of the parameter to false.
        """.stripMargin)
        throw new IllegalArgumentException(s"Unsupported argument: ${opt}")
      }
    })
  }


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


  /**
    * Produce arguments for H2O node based on provided configuration
    *
    * @return array of H2O launcher command line arguments
    */
  def getH2ONodeArgs(conf: H2OConf): Array[String] = {
    (getH2OCommonArgs(conf) ++
      Seq("-log_level", conf.h2oNodeLogLevel, "-baseport", conf.nodeBasePort.toString)).toArray
  }


  private[spark] def guessTotalExecutorSize(sc: SparkContext): Option[Int] = {
    sc.conf.getOption("spark.executor.instances")
      .map(_.toInt)
      .orElse(getCommandArg("--num-executors").map(_.toInt))
      .orElse({
        val sb = sc.schedulerBackend

        val num = sb match {
          case b: LocalSchedulerBackend => Some(1)
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

object InternalBackendUtils extends InternalBackendUtils {

  def toH2OArgs(h2oArgs: Array[String], executors: Array[NodeDesc] = Array()): Array[String] = {
    toH2OArgs(h2oArgs, toFlatFileString(executors))
  }

  private def toH2OArgs(h2oArgs: Array[String], flatFileString: String): Array[String] = {
    h2oArgs ++ Array("-flatfile", SharedBackendUtils.saveAsFile(flatFileString).getAbsolutePath)
  }

  private def toFlatFileString(executors: Array[NodeDesc]): String = {
    executors.map {
      en => s"${en.hostname}:${en.port}"
    }.mkString("\n")
  }
}
