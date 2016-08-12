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

package org.apache.spark.h2o.utils

import water.H2ONode
import water.nbhm.NonBlockingHashMap

/** Helper class containing node ID, hostname and port.
  *
  * @param nodeId  In case of external cluster mode the node ID is ID of H2O Node, in the internal cluster mode the ID
  * is ID of Spark Executor where corresponding instance is located
  * @param hostname hostname of the node
  * @param port port of the node
  */
case class NodeDesc(nodeId: String, hostname: String, port: Int) {
  override def productPrefix = ""
}

object NodeDesc {
  def apply(node: H2ONode): NodeDesc = {
    intern(node)
  }

  private[utils] def fromH2ONode(node: H2ONode): NodeDesc = {
    val ipPort = node.getIpPortString.split(":")
    NodeDesc(node.index().toString, ipPort(0), Integer.parseInt(ipPort(1)))
  }

  private[utils] def intern(node: H2ONode): NodeDesc = {
    var nodeDesc = INTERN_CACHE.get(node)
    if (nodeDesc != null) {
      return nodeDesc
    } else {
      nodeDesc = fromH2ONode(node)
      val oldNodeDesc = INTERN_CACHE.putIfAbsent(node, nodeDesc)
      if (oldNodeDesc != null) {
        return oldNodeDesc
      } else {
        return nodeDesc
      }
    }
  }
  val INTERN_CACHE = new NonBlockingHashMap[H2ONode, NodeDesc]
}
