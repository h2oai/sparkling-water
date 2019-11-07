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

import java.net.URI

import com.google.gson.Gson
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.h2o.H2OConf
import water.api.schemas3.CloudV3

import scala.io.Source

trait H2OContextRestAPIUtils extends H2OContextUtils {


  def getCloudInfoFromNode(node: NodeDesc, conf: H2OConf): CloudV3 = {
    val endpoint = new URI(
      conf.getScheme(),
      null,
      node.hostname,
      node.port,
      conf.contextPath.orNull,
      null,
      null)
    getCloudInfoFromNode(endpoint)
  }


  def getCloudInfo(conf: H2OConf): CloudV3 = {
    val endpoint = getClusterEndpoint(conf)
    getCloudInfoFromNode(endpoint)
  }

  def getNodes(conf: H2OConf): Array[NodeDesc] = {
    val cloudV3 = getCloudInfo(conf)
    getNodes(cloudV3)
  }

  def verifyWebOpen(nodes: Array[NodeDesc], conf: H2OConf): Unit = {
    val nodesWithoutWeb = nodes.flatMap { node =>
      try {
        getCloudInfoFromNode(node, conf)
        None
      } catch {
        case _: H2OClusterNodeNotReachableException => Some(node)
      }
    }
    if (nodesWithoutWeb.nonEmpty) {
      throw new H2OClusterNodeNotReachableException(
        s"""
    The following worker nodes are not reachable, but belong to the cluster:
    ${conf.h2oCluster.get} - ${conf.cloudName.get}:
    ----------------------------------------------
    ${nodesWithoutWeb.map(_.ipPort()).mkString("\n    ")}""", null)
    }
  }

  private def getClusterEndpoint(conf: H2OConf): URI = {
    val uriBuilder = new URIBuilder(s"${conf.getScheme()}://${conf.h2oCluster.get}")
    uriBuilder.setPath(conf.contextPath.orNull)
    uriBuilder.build()
  }

  private def getNodes(cloudV3: CloudV3): Array[NodeDesc] = {
    cloudV3.nodes.zipWithIndex.map { case (node, idx) =>
      val splits = node.ip_port.split(":")
      val ip = splits(0)
      val port = splits(1).toInt
      NodeDesc(idx.toString, ip, port)
    }
  }

  private def getCloudInfoFromNode(endpoint: URI): CloudV3 = {
    val content = readURLContent(endpoint, "3/Cloud")
    new Gson().fromJson(content, classOf[CloudV3])
  }

  private def readURLContent(endpoint: URI, suffix: String): String = {
    try {
      val html = Source.fromURL(s"$endpoint/$suffix")
      val content = html.mkString
      html.close()
      content
    } catch {
      case cause: Exception =>
        throw new H2OClusterNodeNotReachableException(
          s"External H2O node ${endpoint.getHost}:${endpoint.getPort} is not reachable.", cause)
    }
  }

}

class H2OClusterNodeNotReachableException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

object H2OContextRestAPIUtils extends H2OContextRestAPIUtils
