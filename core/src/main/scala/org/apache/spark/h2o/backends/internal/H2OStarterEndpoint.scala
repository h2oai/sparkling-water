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

import org.apache.spark.SparkConf
import org.apache.spark.h2o.H2OConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import water.{H2O, H2ONode}

/**
  * An RpcEndpoint used to start H2O on remote executor
  */
class H2OStarterEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case DistributeNodesInfo(nodes) =>
      nodes.filter(_._2 != H2O.API_PORT).map { pair =>
        val ip = pair._1
        val port = pair._2 + 1 // we send API ports, but to intern node, we need to use internal port
        val h2oNode = H2ONode.intern(InetAddress.getByName(ip), port)
        H2O.addNodeToFlatfile(h2oNode)
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StartH2O(conf, sparkConf, isLocal) =>
      InternalH2OBackend.startH2OWorkerNode(conf, sparkConf, isLocal)
      context.reply(WorkerIpPort(H2O.SELF_ADDRESS.getHostAddress, H2O.API_PORT))
  }
}

case class WorkerIpPort(ip: String, port: Int)

case class DistributeNodesInfo(nodes: Array[(String, Int)])

case class StartH2O(conf: H2OConf, sparkConf: SparkConf, isLocal: Boolean)
