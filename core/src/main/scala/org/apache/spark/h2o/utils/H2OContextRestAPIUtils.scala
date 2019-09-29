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

import com.google.gson.Gson
import water.api.schemas3.CloudV3
import java.net.URI

import org.apache.http.client.utils.URIBuilder
import org.apache.spark.h2o.{H2OConf, H2OContext}

trait H2OContextRestAPIUtils extends H2OContextUtils  {

  def getClusterEndpoint(hc: H2OContext): URI = {
    val uriBuilder = new URIBuilder(s"${hc.getScheme(hc._conf)}://${hc._conf.h2oCluster.get}")
    uriBuilder.setPath(hc._conf.contextPath.orNull)
    uriBuilder.build()
  }

  def getCloudInfoFromNode(node: NodeDesc, conf: H2OConf): CloudV3 = {
    val endpoint = new URI(
      getScheme(conf),
      null,
      node.hostname,
      node.port,
      conf.contextPath.orNull,
      null,
      null)
    getCloudInfoFromNode(endpoint)
  }

  def getCloudInfoFromNode(endpoint: URI): CloudV3 = {
    import scala.io.Source
    val html = Source.fromURL(s"$endpoint/3/Cloud")
    val content = html.mkString
    html.close()
    new Gson().fromJson(content, classOf[CloudV3])
  }

  def getCloudInfo(hc: H2OContext): CloudV3 = {
    val endpoint = getClusterEndpoint(hc)
    getCloudInfoFromNode(endpoint)
  }

  def getNodes(cloudV3: CloudV3): Array[NodeDesc] = {
    cloudV3.nodes.zipWithIndex.map { case (node, idx) =>
      val splits = node.ip_port.split(":")
      val ip = splits(0)
      val port = splits(1).toInt
      NodeDesc(idx.toString, ip, port)
    }
  }

  def getNodes(hc: H2OContext): Array[NodeDesc] = {
    val cloudV3 = getCloudInfo(hc)
    getNodes(cloudV3)
  }

}
