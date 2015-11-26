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

package org.apache.spark.h2o

import java.io.File
import java.net.InetAddress

import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.scheduler.{SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved}
import org.apache.spark.{Accumulable, SparkContext, SparkEnv}
import water.H2OStarter
import water.init.AbstractEmbeddedH2OConfig

import scala.collection.mutable

/**
 * Support methods.
 */
private[spark] object H2OContextUtils {

  /** Helper type expression a tuple of ExecutorId, IP, port */
  type NodeDesc = (String, String, Int)

  /** Generates and distributes a flatfile around Spark cluster.
    *
    * @param distRDD simple RDD to fork execution on.
    * @return list of node descriptions (executorId, executorIP, -1)
    */
  def collectNodesInfo(distRDD: RDD[Int]): Array[NodeDesc] = {
    // Collect flatfile - tuple of (executorId, IP, -1)
    val nodes = distRDD.mapPartitionsWithIndex { (idx, it) =>
      val env = SparkEnv.get
      Iterator.single(
        ( env.executorId,
        // java.net.InetAddress.getLocalHost.getAddress.map(_ & 0xFF).mkString("."),
        // Use existing Akka setup since Spark at this point is already communicating
        getIp(env),
        -1 ) )
    }.collect()
    // Take only unique executors
    nodes.groupBy(_._1).map(_._2.head).toArray.sortWith(_._1 < _._1)
  }

  def getIp(env: SparkEnv) = env.rpcEnv.address.host //settings.config.getString("akka.remote.netty.tcp.hostname")

  def saveAsFile(content: String): File = {
    val tmpDir = createTempDir()
    tmpDir.deleteOnExit()
    val flatFile = new File(tmpDir, "flatfile.txt")
    val p = new java.io.PrintWriter(flatFile)
    try {
      p.print(content)
    } finally {
      p.close()
    }
    flatFile
  }

  def toFlatFileString(executors: Array[NodeDesc]):String = {
    executors.map( en => s"${en._2}:${en._3}").mkString("\n")
  }

  def toH2OArgs(h2oArgs: Array[String], h2oConf: H2OConf, executors: Array[NodeDesc]): Array[String] = {
    toH2OArgs(
      h2oArgs,
      if (h2oConf.useFlatFile) Some(toFlatFileString(executors))
      else None)
  }

  def toH2OArgs(h2oArgs: Array[String], flatFileString: Option[String]): Array[String] = {
    val launcherArgs = flatFileString
      .map(f => saveAsFile(f))
      .map(f => h2oArgs ++ Array("-flatfile", f.getAbsolutePath))
      .getOrElse(h2oArgs)
    launcherArgs
  }

  /**
   * Start H2O nodes on given executors.
   *
   * @param sc  Spark context
   * @param spreadRDD  helper RDD spread over all executors
   * @param numOfExecutors number of executors in Spark cluster
   * @param h2oArgs arguments passed to H2O instances
   * @return flatfile string if flatfile mode is enabled, else None
   */
  def startH2O( sc: SparkContext,
                spreadRDD: RDD[NodeDesc],
                numOfExecutors: Int,
                h2oArgs: Array[String]):Array[NodeDesc] = {

    // Create global accumulator for list of nodes IP:PORT
    val bc = sc.accumulableCollection(new mutable.HashSet[NodeDesc]())
    val isLocal = sc.isLocal
    // Try to launch H2O
    val executorStatus = spreadRDD.map { nodeDesc =>  // RDD partition index
      assert(nodeDesc._2 == getIp(SparkEnv.get),  // Make sure we are running on right node
        s"SpreadRDD failure - IPs are not equal: ${nodeDesc} != (${SparkEnv.get.executorId}, ${getIp(SparkEnv.get)})")
      // Launch the node
      val sparkEnv = SparkEnv.get
      // Define log dir
      def logDir: String = {
        val s = System.getProperty("spark.yarn.app.container.log.dir")
        if (s != null) {
          return s + java.io.File.separator
        }

        if (sparkEnv.conf.contains(H2OConf.PROP_NODE_LOG_DIR._1)) {
          sparkEnv.conf.get(H2OConf.PROP_NODE_LOG_DIR._1)
        } else {
          // Needs to be executed at remote node!
          H2OConf.defaultLogDir
        }
      }
      val executorId = sparkEnv.executorId
      try {
        // Get node this node IP
        val ip = nodeDesc._2
        val launcherArgs = toH2OArgs(
          h2oArgs
            ++ Array("-ip", ip)
            ++ Array("-log_dir", logDir),
          None)
        // Do not launch H2O several times
        if (water.H2O.START_TIME_MILLIS.get() == 0) {
          water.H2O.START_TIME_MILLIS.synchronized {
            if (water.H2O.START_TIME_MILLIS.get() == 0) {
              val t = new Thread("H2O Launcher thread") {
                override def run(): Unit = {
                  water.H2O.setEmbeddedH2OConfig(new SparklingWaterConfig(bc))
                  // Finalize REST API only if running in non-local mode.
                  // In local mode, we are not going to create H2O client
                  // but use executor's H2O instance directly.
                  H2OStarter.start(launcherArgs, !isLocal)
                  // Signal via singleton object that h2o was started on this node
                  H2OStartedSignal.synchronized {
                    H2OStartedSignal.setStarted
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
    val flatFile = bc.value.toArray
    val flatFileString = toFlatFileString(flatFile)
    // Pass flatfile around cluster
    spreadRDD.foreach { nodeDesc =>
      assert(nodeDesc._2 == getIp(SparkEnv.get)) // Make sure we are running on right node
      val executorId = SparkEnv.get.executorId

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

  val TEMP_DIR_ATTEMPTS = 1000

  private def createTempDir(): File = {
    def baseDir = new File(System.getProperty("java.io.tmpdir"))
    def baseName = System.currentTimeMillis() + "-"

    var cnt = 0
    while (cnt < TEMP_DIR_ATTEMPTS) {// infinite loop
      val tempDir = new File(baseDir, baseName + cnt)
      if (tempDir.mkdir()) return tempDir
      cnt += 1
    }
    throw new IllegalStateException(s"Failed to create temporary directory $baseDir / $baseName")
  }
}

/**
 * Embedded config for passing around information of ip and port of created H2O instance.
 * It is using Spark's accumulable variable to collect IP and PORT, and also executor id.
 *
 * @param flatfileBVariable Spark's accumulable variable
 */
private class SparklingWaterConfig(val flatfileBVariable: Accumulable[mutable.HashSet[NodeDesc], NodeDesc])
  extends AbstractEmbeddedH2OConfig with org.apache.spark.Logging {

  /** String containing a flatfile string filled asynchroniously by different thread. */
  @volatile var flatFile:Option[String] = None

  override def notifyAboutEmbeddedWebServerIpPort(ip: InetAddress, port: Int): Unit = {
    val env = SparkEnv.get
    val thisNodeInfo = (env.executorId, getIp(env), port)
    flatfileBVariable.synchronized {
      flatfileBVariable += thisNodeInfo
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

/**
 * Spark environment listener to kill H2O cloud
 * if number of Spark block managers change.
 */
private[h2o]
trait SparkEnvListener extends org.apache.spark.scheduler.SparkListener { self: H2OContext =>

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    println("--------------------> onBlockManagerAdded: " + blockManagerAdded)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    println("--------------------> onBlockManagerRemoved: " + blockManagerRemoved)
  }
}

// JVM private H2O is fully initialized signal.
// Ugly, but what we can do with h2o
private object H2OStartedSignal {
  @volatile private var started = false
  def isStarted = started
  def setStarted: Unit = {
    started = true
  }
}


