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

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.net.URI

import com.google.gson.{ExclusionStrategy, FieldAttributes, GsonBuilder}
import org.apache.commons.io.IOUtils
import org.apache.http.{HttpEntity, HttpHeaders}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.h2o.H2OConf

import scala.reflect.{ClassTag, classTag}

trait RestCommunication {

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
  protected def query[ResultType: ClassTag](
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
  protected def update[ResultType: ClassTag](
      endpoint: URI,
      suffix: String,
      conf: H2OConf,
      skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    request(endpoint, HttpPost.METHOD_NAME, suffix, conf, skippedFields)
  }

  protected def request[ResultType: ClassTag](
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

  protected def downloadBinaryURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
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

  protected def downloadStringURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
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


  private lazy val httpClient = HttpClientBuilder
    .create()
    .setConnectionManager(new PoolingHttpClientConnectionManager)
    .build()

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

  private def readURLContent(endpoint: URI, requestType: String, suffix: String, conf: H2OConf): HttpEntity = {
    try {
      val request = requestType match {
        case HttpGet.METHOD_NAME => new HttpGet(s"$endpoint/$suffix")
        case HttpPost.METHOD_NAME => new HttpPost(s"$endpoint/$suffix")
        case unknown => throw new IllegalArgumentException(s"Unsupported HTTP request type $unknown")
      }
      getCredentials(conf).foreach(creds => request.setHeader(HttpHeaders.AUTHORIZATION, creds))
      val result = retry(3) {
        httpClient.execute(request)
      }
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

  @annotation.tailrec
  private def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1 =>
        Thread.sleep(100)
        retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }
}

abstract class RestApiException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

final class RestApiNotReachableException(msg: String, cause: Throwable) extends RestApiException(msg, cause)

final class RestApiUnauthorisedException(msg: String) extends RestApiException(msg)
