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
import java.net.{HttpURLConnection, URI, URL}

import com.google.gson.{ExclusionStrategy, FieldAttributes, GsonBuilder}
import org.apache.commons.io.IOUtils
import org.apache.spark.h2o.H2OConf

import scala.reflect.{ClassTag, classTag}
import ai.h2o.sparkling.utils.ScalaUtils._
import org.apache.spark.expose.Logging

trait RestCommunication extends Logging {

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
    request(endpoint, "GET", suffix, conf, skippedFields)
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
    request(endpoint, "POST", suffix, conf, skippedFields)
  }

  protected def request[ResultType: ClassTag](
      endpoint: URI,
      requestType: String,
      suffix: String,
      conf: H2OConf,
      skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    withResource(readURLContent(endpoint, requestType, suffix, conf)) { response =>
      val content = IOUtils.toString(response)
      deserialize[ResultType](content, skippedFields)
    }
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
    withResource(readURLContent(endpoint, "GET", suffix, conf)) { input =>
      withResource(new BufferedOutputStream(new FileOutputStream(file))) { output =>
        IOUtils.copy(input, output)
      }
    }
  }

  protected def downloadStringURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
    withResource(readURLContent(endpoint, "GET", suffix, conf)) { input =>
      withResource(new java.io.FileWriter(file)) { output =>
        IOUtils.copy(input, output)
      }
    }
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

  private def urlToString(url: URL) = s"${url.getHost}:${url.getPort}"

  protected def readURLContent(endpoint: URI, requestType: String, suffix: String, conf: H2OConf): InputStream = {
    val suffixWithDelimiter = if (suffix.startsWith("/")) suffix else s"/$suffix"
    val url = endpoint.resolve(suffixWithDelimiter).toURL
    try {
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod(requestType)
      getCredentials(conf).foreach(connection.setRequestProperty("Authorization", _))
      val statusCode = retry(3) {
        connection.getResponseCode()
      }
      statusCode match {
        case HttpURLConnection.HTTP_OK => logInfo(
          s"""External H2O node ${urlToString(url)} successfully responded
             | for the $requestType request on the patch $suffixWithDelimiter.""".stripMargin)
        case HttpURLConnection.HTTP_UNAUTHORIZED => throw new RestApiUnauthorisedException(
          s"""External H2O node ${urlToString(url)} could not be reached because the client is not authorized.
             |Please make sure you have passed valid credentials to the client.
             |Status code $statusCode : ${connection.getResponseMessage()}.""".stripMargin)
        case _ => throw new RestApiNotReachableException(
          s"""External H2O node ${urlToString(url)} responded with
             |status code: $statusCode - ${connection.getResponseMessage()}.""".stripMargin, null)
      }
      connection.getInputStream()
    } catch {
      case e: RestApiException => throw e
      case cause: Exception =>
        throw new RestApiNotReachableException(
          s"""External H2O node ${urlToString(url)} is not reachable.
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
