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

import java.net.InetAddress

import org.apache.spark.h2o.backends.{SharedBackendConf, SharedBackendUtils}
import org.apache.spark.h2o.utils.{NodeDesc, ReflectionUtils}
import org.apache.spark.h2o.{H2OConf, RDD}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkContext, SparkEnv}
import water.H2OStarter
import water.init.AbstractEmbeddedH2OConfig

/**
  * Various helper methods used in the internal backend
  */
private[internal] trait InternalBackendUtils extends SharedBackendUtils {

  def checkUnsupportedSparkOptions(unsupportedSparkOptions: Seq[(String, String)], conf: H2OConf): Unit ={
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

  def toH2OArgs(h2oArgs: Array[String], conf: H2OConf, executors: Array[NodeDesc]): Array[String] = {
    toH2OArgs(
      h2oArgs,
      if (conf.useFlatFile) Some(toFlatFileString(executors))
      else None)
  }

  private[this] def toH2OArgs(h2oArgs: Array[String], flatFileString: Option[String]): Array[String] = {
    val launcherArgs = flatFileString
      .map(f => saveAsFile(f))
      .map(f => h2oArgs ++ Array("-flatfile", f.getAbsolutePath))
      .getOrElse(h2oArgs)
    launcherArgs
  }
  /**
    * Produce arguments for H2O node based on provided configuration
    *
    * @return array of H2O launcher command line arguments
    */
  def getH2ONodeArgs(conf: H2OConf): Array[String] = (getH2OCommonArgs(conf) ++
    Seq("-log_level", conf.h2oNodeLogLevel, "-baseport", conf.nodeBasePort.toString)).toArray


  def toFlatFileString(executors: Array[NodeDesc]): String = {
    executors.map(en => s"${en.hostname}:${en.port}").mkString("\n")
  }

  /**
    * Start H2O nodes on given executors.
    *
    * @param sc  Spark context
    * @param spreadRDD  helper RDD spread over all executors
    * @param numOfExecutors number of executors in Spark cluster
    * @param h2oArgs arguments passed to H2O instances
    * @param networkMask node network mask
    * @return flatfile string if flatfile mode is enabled, else None
    */
  def startH2O( sc: SparkContext,
                spreadRDD: RDD[NodeDesc],
                numOfExecutors: Int,
                h2oArgs: Array[String],
                networkMask: Option[String]): Array[NodeDesc] = {

    // Create global accumulator for list of nodes IP:PORT
    val bc = sc.collectionAccumulator[NodeDesc]
    val isLocal = sc.isLocal
    val userSpecifiedCloudSize = sc.getConf.getOption("spark.executor.instances").map(_.toInt)

    // Try to launch H2O
    val executorStatus = spreadRDD.map { nodeDesc =>  // RDD partition index
      assert(nodeDesc.hostname == getHostname(SparkEnv.get),  // Make sure we are running on right node
        s"SpreadRDD failure - IPs are not equal: ${nodeDesc} != (${SparkEnv.get.executorId}, ${getHostname(SparkEnv.get)})")
      // Launch the node
      val sparkEnv = SparkEnv.get
      // Define log dir
      def logDir: String = {
        val s = System.getProperty("spark.yarn.app.container.log.dir")
        if (s != null) {
          return s + java.io.File.separator
        }
        if (sparkEnv.conf.contains(SharedBackendConf.PROP_NODE_LOG_DIR._1)) {
          sparkEnv.conf.get(SharedBackendConf.PROP_NODE_LOG_DIR._1)
        } else {
          // Needs to be executed at remote node!
          SharedBackendUtils.defaultLogDir(sparkEnv.conf.getAppId)
        }
      }
      val executorId = sparkEnv.executorId
      try {
        // Get this node hostname
        val ip = nodeDesc.hostname
        // Always trust Spark networking except when network mask is specified
        val launcherArgs = toH2OArgs(
          h2oArgs
            ++ networkMask.map(mask => Array("-network", mask)).getOrElse(Array("-ip", ip))
            ++ Array("-log_dir", logDir),
          None)
        // Do not launch H2O several times
        if (water.H2O.START_TIME_MILLIS.get() == 0) {
          water.H2O.START_TIME_MILLIS.synchronized {
            if (water.H2O.START_TIME_MILLIS.get() == 0) {
              val t = new Thread("H2O Launcher thread") {
                override def run(): Unit = {
                  water.H2O.setEmbeddedH2OConfig(new SparklingWaterConfig(bc, if (networkMask.isEmpty) Some(ip) else None))
                  // Finalize REST API only if running in non-local mode.
                  // In local mode, we are not going to create H2O client
                  // but use executor's H2O instance directly.
                  H2OStarter.start(launcherArgs, !isLocal)
                  // Signal via singleton object that h2o was started on this node
                  H2OStartedSignal.synchronized {
                    H2OStartedSignal.setStarted()
                    H2OStartedSignal.notifyAll()
                  }
                }
              }
              t.start()
              // Need to wait since we are using shared but local broadcast variable
              bc.synchronized { bc.wait() }
            }
          }
        }
        (executorId, true)
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          println(
            s""""Cannot start H2O node because: ${e.getMessage}
                | h2o parameters: ${h2oArgs.mkString(",")}
             """.stripMargin)
          (executorId, false)
        }
      }
    }.collect()


    // The accumulable should contain all IP:PORTs from all exeuctors
    if (bc.value.size != numOfExecutors ||
      executorStatus.groupBy(_._1).flatMap( x => x._2.find(_._2)).size != numOfExecutors) {
      throw new RuntimeException(s"Cannot launch H2O on executors: numOfExecutors=${numOfExecutors}, " +
        s"executorStatus=${executorStatus.mkString(",")}")
    }
    // Create flatfile string and pass it around cluster
    val flatFile = bc.value.toArray(new Array[NodeDesc](bc.value.size()))
    val flatFileString = toFlatFileString(flatFile)
    // Pass flatfile around cluster

    spreadRDD.foreach { nodeDesc =>
      val env = SparkEnv.get
      assert(nodeDesc.hostname == getHostname(env), s"nodeDesc=${nodeDesc} == ${getHostname(env)}") // Make sure we are running on right node

      val econf = water.H2O.getEmbeddedH2OConfig().asInstanceOf[SparklingWaterConfig]
      // Setup flatfile for waiting guys
      econf.synchronized {
        econf.flatFile = Option(flatFileString)
        econf.notifyAll()
      }
    }
    // Wait for start of H2O in single JVM
    if (isLocal) {
      H2OStartedSignal.synchronized { while (!H2OStartedSignal.isStarted) H2OStartedSignal.wait() }
    }
    // Return flatfile
    flatFile
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


/**
 * Embedded config for passing around information of ip and port of created H2O instance.
 * It is using Spark's accumulable variable to collect IP and PORT, and also executor id.
 *
 * @param flatfileBVariable Spark's accumulable variable
 */
private class SparklingWaterConfig(val flatfileBVariable: CollectionAccumulator[NodeDesc],
                                   val sparkHostname: Option[String])
  extends AbstractEmbeddedH2OConfig with Logging {

    /** String containing a flatfile string filled asynchronously by different thread. */
    @volatile var flatFile: Option[String] = None

  override def notifyAboutEmbeddedWebServerIpPort(ip: InetAddress, port: Int): Unit = {
    val env = SparkEnv.get
    // assert: ip.isDefined && ip == getHostname(env)
    val hostname = sparkHostname.getOrElse(ip.getHostAddress)
    val thisNodeInfo = NodeDesc(env.executorId, hostname, port)
    flatfileBVariable.synchronized {
      flatfileBVariable.add(thisNodeInfo)
      flatfileBVariable.notifyAll()

    }
  }

    override def notifyAboutCloudSize(ip: InetAddress, port: Int, size: Int): Unit = {
      /* do nothing */
    }

    override def fetchFlatfile(): String = {
      this.synchronized { while (flatFile.isEmpty) this.wait() }

      flatFile.get
    }

    override def providesFlatfile(): Boolean = true

    override def exit(status: Int): Unit = { /* do nothing */ }

    override def print(): Unit = logInfo("""Debug info: NA""")
  }
}

private[internal] object InternalBackendUtils extends InternalBackendUtils

// JVM private H2O is fully initialized signal.
// Ugly, but what we can do with h2o
private object H2OStartedSignal {
  @volatile private var started = false
  def isStarted = started
  def setStarted(): Unit = {
    started = true
  }
}

