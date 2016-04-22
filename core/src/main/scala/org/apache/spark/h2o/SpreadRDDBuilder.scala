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

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend

import scala.annotation.tailrec

/**
  * An H2O specific builder for InvokeOnNodesRDD.
  */
private[spark]
class SpreadRDDBuilder(sc: SparkContext,
                       numExecutorHint: Option[Int] = None) extends {
    val sparkConf = sc.getConf
  } with H2OConf with org.apache.spark.Logging {

  val numExecutors = numH2OWorkers

  def build(): (RDD[NodeDesc], Array[NodeDesc]) = {
    logDebug(s"Building SpreadRDD: numExecutors=${numExecutors}, numExecutorHint=${numExecutorHint}")
    build(numRddRetries, drddMulFactor, 0)
  }

  @tailrec
  private
  def build(nretries: Int,
            mfactor: Int,
            numTriesSame: Int): (RDD[NodeDesc], Array[NodeDesc]) = {
    logDebug(s"Creating RDD for launching H2O nodes (mretries=${nretries}, mfactor=${mfactor}, " +
             s"numTriesSame=${numTriesSame}, backend#isReady=${isBackendReady()}")
    // Get number of available Spark executors, invoke distributed operation and compute
    // number of visible nodes again
    val nSparkExecBefore = numOfSparkExecutors
    // Number of expected workers
    val expectedWorkers = numExecutors.orElse(numExecutorHint).getOrElse(if (nSparkExecBefore > 0) nSparkExecBefore else defaultCloudSize)
    // Create some distributed data
    val spreadRDD = sc.parallelize(0 until mfactor*expectedWorkers, mfactor*expectedWorkers + 1).persist()
    // Collect information about executors in Spark cluster
    val visibleNodes = collectNodesInfo(spreadRDD)
    val numVisibleNodes = visibleNodes.map(_.executorId).distinct.length
    // Number of Spark executors after distributed operation
    val nSparkExecAfter = numOfSparkExecutors
    // Delete RDD
    spreadRDD.unpersist()

    // Decide about visible state
    if ((numVisibleNodes < numExecutors.getOrElse(-1) || nSparkExecAfter != nSparkExecBefore)
        && nretries == 0) {
      // We tried many times, but we were not able to get right number of executors
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors:
            | Expected number of H2O workers is ${numExecutors} (hint=${numExecutorHint})
            | Detected number of Spark workers is $numVisibleNodes
            | Num of Spark executors before is $nSparkExecBefore
            | Num of Spark executors after is $nSparkExecAfter
            |""".stripMargin
      )
    } else if (nSparkExecAfter != nSparkExecBefore || nSparkExecAfter != numVisibleNodes) {
      // We detected change in number of executors
      logInfo(s"Detected ${nSparkExecBefore} before, and ${nSparkExecAfter} spark executors after, backend#isReady=${isBackendReady()}! Retrying again...")
      build(nretries - 1, 2*mfactor, 0)
    } else if ((!numExecutors.isDefined && numTriesSame == subseqTries)
               || (!numExecutors.isDefined && numVisibleNodes == numExecutorHint.getOrElse(-1))
               || (numExecutors.isDefined && numExecutors.get == numVisibleNodes)
               || (numExecutors.isDefined && numTriesSame == subseqTries)) {
      logInfo(s"Detected ${numVisibleNodes} spark executors for ${numExecutors} H2O workers!")
      (new InvokeOnNodesRDD(visibleNodes, sc), visibleNodes)
    } else {
      logInfo(s"Detected ${numVisibleNodes} spark executors for ${numExecutors} H2O workers, backend#isReady=${isBackendReady()}! Retrying again...")
      build(nretries-1, mfactor, numTriesSame + 1)
    }
  }

  /** Generates and distributes a flatfile around Spark cluster.
    *
    * @param distRDD simple RDD to fork execution on.
    * @return list of node descriptions NodeDesc(executorId, executorHostName, -1)
    */
  def collectNodesInfo(distRDD: RDD[Int]): Array[NodeDesc] = {
    // Collect flatfile - tuple of (executorId, IP, -1)
    val nodes = distRDD.mapPartitionsWithIndex { (idx, it) =>
      val env = SparkEnv.get
      Iterator.single(NodeDesc(env.executorId, getHostname(env), -1)
      )
    }.collect()
    // Take only unique executors
    nodes.groupBy(_.executorId).map(_._2.head).toArray.sortWith(_.executorId < _.executorId)
  }

  /**
    * Return number of registered Spark executors
    */
  private def numOfSparkExecutors = if (sc.isLocal) 1 else {
    val sb = sc.schedulerBackend
    sb match {
      case b: LocalBackend => 1
      case b: CoarseGrainedSchedulerBackend => b.numExistingExecutors
      case _ => sc.getExecutorStorageStatus.length - 1
    }
  }

  private def isBackendReady() = sc.schedulerBackend.isReady()
}
