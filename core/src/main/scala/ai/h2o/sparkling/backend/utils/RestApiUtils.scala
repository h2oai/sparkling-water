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

package ai.h2o.sparkling.backend.utils

import java.net.URI

import org.apache.http.client.utils.URIBuilder
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.api.schemas3._

trait RestApiUtils extends RestCommunication {

  def isRestAPIBased(hc: Option[H2OContext] = None): Boolean = {
    isRestAPIBased(hc.getOrElse(H2OContext.ensure()).getConf)
  }

  def isRestAPIBased(conf: H2OConf): Boolean = {
    conf.get("spark.ext.h2o.rest.api.based.client", "false") == "true"
  }

  def isRestAPIBased(hc: H2OContext): Boolean = isRestAPIBased(Some(hc))

  def getPingInfo(conf: H2OConf): PingV3 = {
    val endpoint = getClusterEndpoint(conf)
    query[PingV3](endpoint, "/3/Ping", conf)
  }

  def shutdownCluster(conf: H2OConf): Unit = {
    val endpoint = getClusterEndpoint(conf)
    update[ShutdownV3](endpoint, "/3/Shutdown", conf)
  }

  def resolveNodeEndpoint(node: NodeDesc, conf: H2OConf): URI = {
    new URI(
      conf.getScheme(),
      null,
      node.hostname,
      node.port,
      conf.contextPath.orNull,
      null,
      null)
  }

  def getCloudInfoFromNode(node: NodeDesc, conf: H2OConf): CloudV3 = {
    val endpoint = resolveNodeEndpoint(node, conf)
    getCloudInfoFromNode(endpoint, conf)
  }

  def getClusterInfo(conf: H2OConf): CloudV3 = {
    val endpoint = getClusterEndpoint(conf)
    getCloudInfoFromNode(endpoint, conf)
  }

  def getNodes(conf: H2OConf): Array[NodeDesc] = {
    val cloudV3 = getClusterInfo(conf)
    getNodes(cloudV3)
  }

  def getLeaderNode(conf: H2OConf): NodeDesc = {
    val cloudV3 = getClusterInfo(conf)
    val nodes = getNodes(cloudV3)
    if (cloudV3.leader_idx < 0 || cloudV3.leader_idx >= nodes.length) {
      throw new RuntimeException(
        s"The leader index '${cloudV3.leader_idx}' doesn't correspond to the size of the H2O cluster ${nodes.length}.")
    }
    nodes(cloudV3.leader_idx)
  }

  def getClusterEndpoint(conf: H2OConf): URI = {
    val uriBuilder = new URIBuilder(s"${conf.getScheme()}://${conf.h2oCluster.get}")
    uriBuilder.setPath(conf.contextPath.orNull)
    uriBuilder.build()
  }

  def getNodes(cloudV3: CloudV3): Array[NodeDesc] = {
    cloudV3.nodes.zipWithIndex.map { case (node, idx) =>
      val splits = node.ip_port.split(":")
      val ip = splits(0)
      val port = splits(1).toInt
      NodeDesc(idx.toString, ip, port)
    }
  }

  def getCloudInfoFromNode(endpoint: URI, conf: H2OConf): CloudV3 = {
    query[CloudV3](endpoint, "/3/Cloud", conf)
  }

  private def executeStringRapidsExpression(conf: H2OConf, expression: String): String = {
    val endpoint = getClusterEndpoint(conf)
    val params = Map("ast" -> expression)
    val result = update[RapidsStringV3](endpoint: URI, "/99/Rapids", conf, params)
    result.string
  }

  def getTimeZone(conf: H2OConf): String = executeStringRapidsExpression(conf, "(getTimeZone)")

  def setTimeZone(conf: H2OConf, timezone: String): Unit = {
    val expression = s"""(setTimeZone "$timezone")"""
    executeStringRapidsExpression(conf, expression)
  }
}

object RestApiUtils extends RestApiUtils
