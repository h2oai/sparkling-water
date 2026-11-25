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

package ai.h2o.sparkling.backend

import water.H2ONode
import water.nbhm.NonBlockingHashMap

/** Helper class containing node ID, hostname and port.
  *
  * @param nodeId   In case of external cluster mode the node ID is ID of H2O Node, in the internal cluster mode the ID
  *                 is ID of Spark Executor where corresponding instance is located
  * @param hostname hostname of the node
  * @param port     port of the node
  */
case class NodeDesc(nodeId: String, hostname: String, port: Int) {
  override def productPrefix = ""

  def ipPort(): String = NodeDesc.formatIpPort(hostname, port)
}

object NodeDesc {
  def apply(node: H2ONode): NodeDesc = {
    intern(node)
  }

  private def fromH2ONode(node: H2ONode): NodeDesc = {
    val (ip, port) = parseIpPort(node.getIpPortString)
    NodeDesc(node.index().toString, ip, port)
  }

  /**
    * Parses an "ip:port" string. IPv6 addresses are supported both with and without
    * square brackets ("[2001:db8::1]:54321" or "2001:db8::1:54321"); the returned host never contains brackets.
    */
  private[sparkling] def parseIpPort(ipPort: String): (String, Int) = {
    val separatorIdx = ipPort.lastIndexOf(':')
    require(separatorIdx > 0, s"Expected a string in the format 'ip:port', but got '$ipPort'")
    val host = ipPort.substring(0, separatorIdx).stripPrefix("[").stripSuffix("]")
    val port = Integer.parseInt(ipPort.substring(separatorIdx + 1))
    (host, port)
  }

  /** Formats host and port as "host:port", wrapping IPv6 addresses in square brackets so the result is URL-safe. */
  private[sparkling] def formatIpPort(host: String, port: Int): String = {
    if (host.contains(":") && !host.startsWith("[")) s"[$host]:$port" else s"$host:$port"
  }

  private def intern(node: H2ONode): NodeDesc = {
    var nodeDesc = INTERN_CACHE.get(node)
    if (nodeDesc != null) {
      nodeDesc
    } else {
      nodeDesc = fromH2ONode(node)
      val oldNodeDesc = INTERN_CACHE.putIfAbsent(node, nodeDesc)
      if (oldNodeDesc != null) {
        oldNodeDesc
      } else {
        nodeDesc
      }
    }
  }

  private val INTERN_CACHE = new NonBlockingHashMap[H2ONode, NodeDesc]
}
