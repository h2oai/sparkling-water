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

package org.apache.spark.h2o.backends.external

import java.io.{BufferedOutputStream, File, FileOutputStream, InputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import ai.h2o.sparkling.frame.{H2OChunk, H2OColumn, H2OFrame}
import com.google.gson.{ExclusionStrategy, FieldAttributes, GsonBuilder}
import org.apache.commons.io.IOUtils
import org.apache.http.{HttpEntity, HttpHeaders}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.NodeDesc
import water.api.schemas3.FrameChunksV3.FrameChunkV3
import water.api.schemas3.FrameV3.ColV3
import water.api.schemas3._

import scala.reflect.{ClassTag, _}


trait RestApiUtils {

  def lockCloud(conf: H2OConf): Unit = {
    val endpoint = getClusterEndpoint(conf)
    update[CloudLockV3](endpoint, "3/CloudLock", conf)
  }

  def shutdownCluster(conf: H2OConf): Unit = {
    val endpoint = getClusterEndpoint(conf)
    update[ShutdownV3](endpoint, "3/Shutdown", conf)
  }

  def getCloudInfoFromNode(node: NodeDesc, conf: H2OConf): CloudV3 = {
    val endpoint = new URI(
      conf.getScheme(),
      null,
      node.hostname,
      node.port,
      conf.contextPath.orNull,
      null,
      null)
    getCloudInfoFromNode(endpoint, conf)
  }

  def downloadLogs(destinationDir: String, logContainer: String, conf: H2OConf): String = {
    val endpoint = getClusterEndpoint(conf)
    val file = new File(destinationDir, s"${logFileName()}.${logContainer.toLowerCase}")
    val logEndpoint = s"3/Logs/download/$logContainer"
    logContainer match {
      case "LOG" =>
        downloadStringURLContent(endpoint, logEndpoint, conf, file)
      case "ZIP" =>
        downloadBinaryURLContent(endpoint, logEndpoint, conf, file)
    }
    file.getAbsolutePath
  }

  private def logFileName(): String = {
    val pattern = "yyyyMMdd_hhmmss"
    val formatter = new SimpleDateFormat(pattern)
    val now = formatter.format(new Date)
    s"h2ologs_$now"
  }

  def getPingInfo(conf: H2OConf): PingV3 = {
    val endpoint = getClusterEndpoint(conf)
    query[PingV3](endpoint, "3/Ping", conf)
  }

  def getCloudInfo(conf: H2OConf): CloudV3 = {
    val endpoint = getClusterEndpoint(conf)
    getCloudInfoFromNode(endpoint, conf)
  }

  def getNodes(conf: H2OConf): Array[NodeDesc] = {
    val cloudV3 = getCloudInfo(conf)
    getNodes(cloudV3)
  }

  def getLeaderNode(conf: H2OConf): NodeDesc = {
    val cloudV3 = getCloudInfo(conf)
    val nodes = getNodes(cloudV3)
    if (cloudV3.leader_idx < 0 || cloudV3.leader_idx >= nodes.length) {
      throw new RuntimeException(
        s"The leader index '${cloudV3.leader_idx}' doesn't correspond to the size of the H2O cluster ${nodes.length}.")
    }
    nodes(cloudV3.leader_idx)
  }

  def getFrame(conf: H2OConf, frameId: String): H2OFrame = {
    val endpoint = getClusterEndpoint(conf)
    val frames = query[FramesV3](
      endpoint,
      s"3/Frames/$frameId/summary?row_count=0",
      conf,
      Seq((classOf[FrameV3], "chunk_summary"), (classOf[FrameV3], "distribution_summary")))
    val frame = frames.frames(0)
    val frameChunks = query[FrameChunksV3](endpoint, s"3/FrameChunks/$frameId", conf)
    val clusterNodes = getNodes(getCloudInfoFromNode(endpoint, conf))

    H2OFrame(
      frameId = frame.frame_id.name,
      columns = frame.columns.map(convertColumn),
      chunks = frameChunks.chunks.map(convertChunk(_, clusterNodes)))
  }

  private def convertColumn(sourceColumn: ColV3): H2OColumn = {
    H2OColumn(
      name = sourceColumn.label,
      dataType = sourceColumn.`type`,
      min = sourceColumn.mins(0),
      max = sourceColumn.maxs(0),
      mean = sourceColumn.mean,
      sigma = sourceColumn.sigma,
      numberOfZeros = sourceColumn.zero_count,
      numberOfMissingElements = sourceColumn.missing_count,
      percentiles = sourceColumn.percentiles,
      domain = sourceColumn.domain,
      domainCardinality = sourceColumn.domain_cardinality)
  }

  private def convertChunk(sourceChunk: FrameChunkV3, clusterNodes: Array[NodeDesc]): H2OChunk = {
    H2OChunk(
      index = sourceChunk.chunk_id,
      numberOfRows = sourceChunk.row_count,
      location = clusterNodes(sourceChunk.node_idx))
  }

  def verifyWebOpen(nodes: Array[NodeDesc], conf: H2OConf): Unit = {
    val nodesWithoutWeb = nodes.flatMap { node =>
      try {
        getCloudInfoFromNode(node, conf)
        None
      } catch {
        case cause: RestApiException => Some((node, cause))
      }
    }
    if (nodesWithoutWeb.nonEmpty) {
      throw new H2OClusterNotReachableException(
        s"""
    The following worker nodes are not reachable, but belong to the cluster:
    ${conf.h2oCluster.get} - ${conf.cloudName.get}:
    ----------------------------------------------
    ${nodesWithoutWeb.map(_._1.ipPort()).mkString("\n    ")}""", nodesWithoutWeb.head._2)
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

  private def getCloudInfoFromNode(endpoint: URI, conf: H2OConf): CloudV3 = {
    query[CloudV3](endpoint, "3/Cloud", conf)
  }

  /**
    *
    * @param endpoint      An address of H2O node with exposed REST endpoint
    * @param suffix        REST relative path representing a specific call
    * @param conf          H2O conf object
    * @param skippedFields The list of field specifications that are skipped during deserialization. The specification
    *                      consists of the class containing the field and the field name.
    * @tparam ResultType A type that the result will be deserialized to
    * @return A deserialized object
    */
  private def query[ResultType: ClassTag](
                                           endpoint: URI,
                                           suffix: String,
                                           conf: H2OConf,
                                           skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    request(endpoint, HttpGet.METHOD_NAME, suffix, conf, skippedFields)
  }


  /**
    *
    * @param endpoint      An address of H2O node with exposed REST endpoint
    * @param suffix        REST relative path representing a specific call
    * @param conf          H2O conf object
    * @param skippedFields The list of field specifications that are skipped during deserialization. The specification
    *                      consists of the class containing the field and the field name.
    * @tparam ResultType A type that the result will be deserialized to
    * @return A deserialized object
    */
  private def update[ResultType: ClassTag](
                                            endpoint: URI,
                                            suffix: String,
                                            conf: H2OConf,
                                            skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    request(endpoint, HttpPost.METHOD_NAME, suffix, conf, skippedFields)
  }

  private def request[ResultType: ClassTag](
                                             endpoint: URI,
                                             requestType: String,
                                             suffix: String,
                                             conf: H2OConf,
                                             skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    val response = readURLContent(endpoint, requestType, suffix, conf)
    val content = IOUtils.toString(response.getContent)
    try {
      EntityUtils.consume(response)
    } catch {
      case _: Throwable =>
    }
    deserialize[ResultType](content, skippedFields)
  }

  private def deserialize[ResultType: ClassTag](content: String, skippedFields: Seq[(Class[_], String)]): ResultType = {
    val builder = new GsonBuilder()
    val exclusionStrategy = new ExclusionStrategy() {
      override def shouldSkipField(f: FieldAttributes): Boolean = {
        skippedFields.exists {
          case (clazz: Class[_], fieldName: String) => clazz == f.getDeclaringClass && fieldName == f.getName
        }
      }

      override def shouldSkipClass(incomingClass: Class[_]): Boolean = false
    }
    builder.addDeserializationExclusionStrategy(exclusionStrategy)
    builder.create().fromJson(content, classTag[ResultType].runtimeClass)
  }

  private def downloadBinaryURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
    val response = readURLContent(endpoint, HttpGet.METHOD_NAME, suffix, conf)
    val output = new BufferedOutputStream(new FileOutputStream(file))
    IOUtils.copy(response.getContent, output)
    try {
      EntityUtils.consume(response)
    } catch {
      case _: Throwable =>
    }
    output.close()
  }

  private def downloadStringURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
    val response = readURLContent(endpoint, HttpGet.METHOD_NAME, suffix, conf)
    val output = new java.io.FileWriter(file)
    IOUtils.copy(response.getContent, output)
    try {
      EntityUtils.consume(response)
    } catch {
      case _: Throwable =>
    }
    output.close()
  }

  private def getCredentials(conf: H2OConf): Option[String] = {
    val username = conf.userName
    val password = conf.password
    if (username.isDefined && password.isDefined) {
      val userpass = s"${username.get}:${password.get}"
      Some("Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes))
    } else {
      None
    }
  }

  private lazy val httpClient = HttpClientBuilder
    .create()
    .setConnectionManager(new PoolingHttpClientConnectionManager)
    .build()

  private def readURLContent(endpoint: URI, requestType: String, suffix: String, conf: H2OConf): HttpEntity = {
    try {
      val request = requestType match {
        case HttpGet.METHOD_NAME => new HttpGet(s"$endpoint/$suffix")
        case HttpPost.METHOD_NAME => new HttpPost(s"$endpoint/$suffix")
        case unknown => throw new IllegalArgumentException(s"Unsupported HTTP request type $unknown")
      }
      getCredentials(conf).foreach(creds => request.setHeader(HttpHeaders.AUTHORIZATION, creds))
      val result = httpClient.execute(request)
      val statusCode = result.getStatusLine.getStatusCode
      statusCode match {
        case 401 => throw new RestApiUnauthorisedException(
          s"""External H2O node ${endpoint.getHost}:${endpoint.getPort} could not be reached because the client is not authorized.
             |Please make sure you have passed valid credentials to the client.
             |Status code $statusCode : ${result.getStatusLine.getReasonPhrase}.""".stripMargin)
        case _ =>
      }
      result.getEntity
    } catch {
      case e: RestApiException => throw e
      case cause: Exception =>
        throw new RestApiNotReachableException(
          s"""External H2O node ${endpoint.getHost}:${endpoint.getPort} is not reachable.
             |Please verify that you are passing ip and port of existing cluster node and the cluster
             |is running with web enabled.""".stripMargin, cause)
    }
  }
}

abstract class RestApiException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

final class RestApiNotReachableException(msg: String, cause: Throwable) extends RestApiException(msg, cause)

final class RestApiUnauthorisedException(msg: String) extends RestApiException(msg)

object RestApiUtils extends RestApiUtils
