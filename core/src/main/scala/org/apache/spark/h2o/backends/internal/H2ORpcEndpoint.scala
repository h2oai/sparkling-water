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

import ai.h2o.sparkling.backend.NodeDesc
import org.apache.spark.h2o.H2OConf
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import water.util.Log
import water.{H2O, H2ONode}

import scala.collection.JavaConverters._

/**
  * An RpcEndpoint used for communication between H2O client and H2O worker nodes on remote executors.
  * This endpoint is started on each Spark executor where H2O worker will be running.
  */
class H2ORpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case FlatFileMsg(nodes, portOffset) =>
      nodes.map { pair =>
        val ip = pair.hostname
        // FlatFile contains API ports, but to directly add a node, we need to use internal port which is formed as
        // API_PORT + PORT_OFFSET
        val internalH2OPort = pair.port + portOffset
        val h2oNode = H2ONode.intern(InetAddress.getByName(ip), internalH2OPort)
        Log.info(s"Adding $h2oNode to ${H2O.SELF}'s flatfile")
        H2O.addNodeToFlatfile(h2oNode)
      }
      Log.info(s"Full flatfile: ${H2O.getFlatfile.asScala.mkString(", ")}")

    case StopEndpointMsg =>
      this.stop()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StartH2OWorkersMsg(conf) =>
      val nodeDesc = InternalH2OBackend.startH2OWorker(conf)
      context.reply(nodeDesc)
  }
}

case class StopEndpointMsg()

case class FlatFileMsg(nodes: Array[NodeDesc], portOffset: Int)

case class StartH2OWorkersMsg(conf: H2OConf)
