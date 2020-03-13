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

import ai.h2o.sparkling.backend.utils.SharedBackendUtils
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.{SparkConf, SparkEnv}

import scala.annotation.tailrec

/**
  * Start RPC endpoint on all discovered executors. These RPC endpoints are later used to start
  * H2O on remote executors.
  */
private[spark]
class SpreadRDDBuilder(@transient private val hc: H2OContext,
                       numExecutorHint: Option[Int] = None) extends Logging {
  @transient private val sc = hc.sparkContext
  private val conf = hc.getConf
  private val isLocal = sc.isLocal
  val sparkConf: SparkConf = sc.conf
  private val numExecutors = conf.numH2OWorkers

  def build(): Array[RpcEndpointRef] = {
    logDebug(s"Building SpreadRDD: numExecutors=$numExecutors, numExecutorHint=$numExecutorHint")
    build(conf.numRddRetries, conf.drddMulFactor, 0)
  }

  @tailrec
  private def build(nretries: Int, mfactor: Int, numTriesSame: Int): Array[RpcEndpointRef] = {
    logDebug(s"Creating RDD for launching H2O nodes (mretries=$nretries, mfactor=$mfactor, " +
      s"numTriesSame=$numTriesSame, backend#isReady=${isBackendReady()}")
    // Get number of available Spark executors, invoke distributed operation and compute
    // number of visible nodes again
    val nSparkExecBefore = numOfSparkExecutors
    // Number of expected workers
    val expectedWorkers = numExecutors.orElse(numExecutorHint).getOrElse(if (nSparkExecBefore > 0) nSparkExecBefore else conf.defaultCloudSize)

    // Create some distributed data
    val spreadRDD = sc.parallelize(0 until mfactor * expectedWorkers, mfactor * expectedWorkers + 1)

    // Start RPC Endpoint on all worker nodes
    val endpoints = spreadRDD.mapPartitions { _ => Iterator.single(RpcReferenceCache.getRef(sparkConf)) }.distinct().collect()

    val currentWorkers = endpoints.length
    // Number of Spark executors after distributed operation
    val nSparkExecAfter = numOfSparkExecutors

    // Decide about visible state
    if ((currentWorkers < expectedWorkers || nSparkExecAfter != nSparkExecBefore) && nretries == 0) {
      // We tried many times, but we were not able to get right number of executors
      throw new IllegalArgumentException(
        s"""Cannot execute H2O on all Spark executors:
           | Expected number of H2O workers is $numExecutorHint
           | Detected number of Spark workers is $currentWorkers
           | Num of Spark executors before is $nSparkExecBefore
           | Num of Spark executors after is $nSparkExecAfter
           |""".stripMargin
      )
    } else if (nSparkExecAfter != nSparkExecBefore || nSparkExecAfter != currentWorkers) {
      // We detected change in number of executors
      logInfo(s"Detected $nSparkExecBefore before, and $nSparkExecAfter Spark executors after, backend#isReady=${isBackendReady()}! Retrying again...")
      build(nretries - 1, 2 * mfactor, 0)
    } else if ((numTriesSame == conf.subseqTries)
      || (numExecutors.isEmpty && currentWorkers == expectedWorkers)
      || (numExecutors.isDefined && numExecutors.get == currentWorkers)) {
      logInfo(s"Detected $currentWorkers spark executors for $expectedWorkers H2O workers!")
      endpoints
    } else {
      logInfo(s"Detected $currentWorkers spark executors for $expectedWorkers H2O workers, backend#isReady=${isBackendReady()}! Retrying again...")
      build(nretries - 1, mfactor, numTriesSame + 1)
    }
  }

  /**
    * Return number of registered Spark executors
    */
  private def numOfSparkExecutors = if (isLocal) 1 else {
    val sb = sc.schedulerBackend
    sb match {
      case _: LocalSchedulerBackend => 1
      case b: CoarseGrainedSchedulerBackend => b.getExecutorIds.length
      case _ => SparkEnv.get.blockManager.master.getStorageStatus.length - 1
    }
  }

  private def isBackendReady() = sc.schedulerBackend.isReady()
}

object RpcReferenceCache extends SharedBackendUtils {
  private object Lock
  private val rpcServiceName = s"sparkling-water-h2o-start-${SparkEnv.get.executorId}"
  private val rpcEndpointName = "h2o"
  private var ref: RpcEndpointRef = _

  def getRef(conf: SparkConf): RpcEndpointRef = Lock.synchronized {
    if (ref == null) {
      ref = startEndpointOnH2OWorker(conf)
    }
    ref
  }

  private def startEndpointOnH2OWorker(conf: SparkConf): RpcEndpointRef = {
    val securityMgr = SparkEnv.get.securityManager
    val rpcEnv = RpcEnv.create(rpcServiceName, getHostname(SparkEnv.get), 0, conf, securityMgr)
    val endpoint = new H2ORpcEndpoint(rpcEnv)
    rpcEnv.setupEndpoint(rpcEndpointName, endpoint)
  }
}
