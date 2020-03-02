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

package ai.h2o.sparkling.backend.external

import java.io._
import java.net.{HttpURLConnection, URI, URL, URLEncoder}

import ai.h2o.sparkling.utils.FinalizingOutputStream
import ai.h2o.sparkling.utils.ScalaUtils._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.{ExclusionStrategy, FieldAttributes, Gson, GsonBuilder}
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf

import scala.reflect.{ClassTag, classTag}

trait RestCommunication extends Logging {

  /**
   *
   * @param endpoint      An address of H2O node with exposed REST endpoint
   * @param suffix        REST relative path representing a specific call
   * @param conf          H2O conf object
   * @param params        Query parameters
   * @param skippedFields The list of field specifications that are skipped during deserialization. The specification
   *                      consists of the class containing the field and the field name.
   * @tparam ResultType A type that the result will be deserialized to
   * @return A deserialized object
   */
  def query[ResultType: ClassTag](
                                   endpoint: URI,
                                   suffix: String,
                                   conf: H2OConf,
                                   params: Map[String, Any] = Map.empty,
                                   skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    request(endpoint, "GET", suffix, conf, params, skippedFields)
  }


  /**
   *
   * @param endpoint      An address of H2O node with exposed REST endpoint
   * @param suffix        REST relative path representing a specific call
   * @param conf          H2O conf object
   * @param params        Query parameters
   * @param skippedFields The list of field specifications that are skipped during deserialization. The specification
   *                      consists of the class containing the field and the field name.
   * @tparam ResultType A type that the result will be deserialized to
   * @return A deserialized object
   */
  def update[ResultType: ClassTag](
                                    endpoint: URI,
                                    suffix: String,
                                    conf: H2OConf,
                                    params: Map[String, Any] = Map.empty,
                                    skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    request(endpoint, "POST", suffix, conf, params, skippedFields)
  }

  /**
   *
   * @param endpoint An address of H2O node with exposed REST endpoint
   * @param suffix   REST relative path representing a specific call
   * @param conf     H2O conf object
   * @param params   Query parameters
   * @return HttpUrlConnection facilitating the insertion and holding the outputStream
   */
  protected def insert(
                        endpoint: URI,
                        suffix: String,
                        conf: H2OConf,
                        streamWrapper: OutputStream => OutputStream = identity,
                        params: Map[String, Any] = Map.empty): OutputStream = {
    val url = resolveUrl(endpoint, s"$suffix?${stringifyParams(params)}")
    try {
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      val requestMethod = "PUT"
      connection.setRequestMethod(requestMethod)
      connection.setDoOutput(true)
      connection.setChunkedStreamingMode(-1) // -1 to use default size
      setHeaders(connection, conf, requestMethod, params)
      val outputStream = connection.getOutputStream()
      val wrappedStream = streamWrapper(outputStream)
      new FinalizingOutputStream(wrappedStream, () => checkResponseCode(connection))
    } catch {
      case e: Exception => throwRestApiNotReachableException(url, e)
    }
  }



  def request[ResultType: ClassTag](
                                     endpoint: URI,
                                     requestType: String,
                                     suffix: String,
                                     conf: H2OConf,
                                     params: Map[String, Any] = Map.empty,
                                     skippedFields: Seq[(Class[_], String)] = Seq.empty): ResultType = {
    withResource(readURLContent(endpoint, requestType, suffix, conf, params)) { response =>
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
          case _ => false
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

  private def stringifyPrimitiveParam(value: Any): String = {
    val charset = "UTF-8"
    value match {
      case v: Boolean => v.toString
      case v: Byte => v.toString
      case v: Int => v.toString
      case v: Long => v.toString
      case v: Float => v.toString
      case v: Double => v.toString
      case v: String => URLEncoder.encode(v, charset)
      case unknown => throw new RuntimeException(s"Unsupported parameter '$unknown' of type ${unknown.getClass}")
    }
  }

  private def isPrimitiveType(value: Any): Boolean = {
    value match {
      case _: Boolean => true
      case _: Byte => true
      case _: Int => true
      case _: Long => true
      case _: Float => true
      case _: Double => true
      case _: String => true
      case unknown => throw new RuntimeException(s"Unsupported parameter '$unknown' of type ${unknown.getClass}")
    }
  }

  private def stringifyArray(arr: Array[_]): String = {
    arr.map(stringify).mkString("[", ",", "]")
  }

  private def stringifyMap(map: scala.collection.immutable.Map[_, _]): String = {
    val arr = map.filter { case (_, value) => value != null }.toSeq.map(pair => s"{'key': ${pair._1}, 'value':${stringify(pair._2)}").toArray
    stringifyArray(arr)
  }

  private def stringify(value: Any): String = {
    import scala.collection.JavaConversions._
    value match {
      case map: java.util.AbstractMap[_, _] => stringifyMap(map.toMap)
      case map: scala.collection.immutable.Map[_, _] => stringifyMap(map)
      case arr: Array[_] => stringifyArray(arr)
      case primitive if isPrimitiveType(primitive) => stringifyPrimitiveParam(primitive)
      case unknown => throw new RuntimeException(s"Unsupported parameter '$unknown' of type ${unknown.getClass}")
    }
  }

  private def stringifyParams(params: Map[String, Any] = Map.empty): String = {
    params.filter { case (_, value) => value != null }
      .map { case (name, value) =>
        val encodedValue = stringify(value)
        s"$name=$encodedValue"
      }.mkString("&")
  }

  private def resolveUrl(endpoint: URI, suffix: String): URL = {
    val suffixWithDelimiter = if (suffix.startsWith("/")) suffix else s"/$suffix"
    endpoint.resolve(suffixWithDelimiter).toURL
  }

  private def setHeaders(connection: HttpURLConnection, conf: H2OConf, requestType: String, params: Map[String, Any], asJSON: Boolean = false): Unit = {
    getCredentials(conf).foreach(connection.setRequestProperty("Authorization", _))

    if (params.nonEmpty && requestType == "POST") {
      val paramsAsBytes = if (asJSON) {
        val jsonString = new ObjectMapper().registerModule(DefaultScalaModule).writeValueAsString(params)
        val bytes = jsonString.getBytes("UTF-8")
        connection.setRequestProperty("Content-Type", "application/json")
        bytes
      } else {
        val bytes = stringifyParams(params).getBytes("UTF-8")
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        bytes
      }
      connection.setRequestProperty("charset", "UTF-8")
      connection.setRequestProperty("Content-Length", Integer.toString(paramsAsBytes.length))
      connection.setDoOutput(true)
      withResource(new DataOutputStream(connection.getOutputStream())) { writer =>
        writer.write(paramsAsBytes)
      }
    }
  }

  protected def readURLContent(endpoint: URI,
                               requestType: String,
                               suffix: String,
                               conf: H2OConf, params: Map[String, Any] = Map.empty,
                               asJSON: Boolean = false): InputStream = {
    val suffixWithParams = if (params.nonEmpty && (requestType == "GET")) s"$suffix?${stringifyParams(params)}" else suffix
    val url = resolveUrl(endpoint, suffixWithParams)
    try {
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod(requestType)
      setHeaders(connection, conf, requestType, params, asJSON)
      checkResponseCode(connection)
      connection.getInputStream()
    } catch {
      case e: RestApiException => throw e
      case cause: Exception => throwRestApiNotReachableException(url, cause)
    }
  }

  def checkResponseCode(connection: HttpURLConnection): Unit = {
    val url = connection.getURL
    val requestType = connection.getRequestMethod
    val statusCode = retry(3) {
      connection.getResponseCode()
    }
    statusCode match {
      case HttpURLConnection.HTTP_OK => logInfo(
        s"External H2O node $url successfully responded for the $requestType.")
      case HttpURLConnection.HTTP_UNAUTHORIZED => throw new RestApiUnauthorisedException(
        s"""External H2O node ${urlToString(url)} could not be reached because the client is not authorized.
           |Please make sure you have passed valid credentials to the client.
           |Status code $statusCode : ${connection.getResponseMessage()}.""".stripMargin)
      case _ => throw new RestApiCommunicationException(
        s"""External H2O node ${urlToString(url)} responded with
           |Status code: $statusCode : ${connection.getResponseMessage()}
           |Server error: ${getServerError(connection)}""".stripMargin)
    }
  }

  private def getServerError(connection: HttpURLConnection): String = {
    withResource(connection.getErrorStream) { errorStream =>
      if (errorStream == null) {
        "No error"
      } else {
        IOUtils.toString(errorStream)
      }
    }
  }

  private def throwRestApiNotReachableException(url: URL, e: Exception) = {
    throw new RestApiNotReachableException(
      s"""External H2O node ${urlToString(url)} is not reachable.
         |Please verify that you are passing ip and port of existing cluster node and the cluster
         |is running with web enabled.""".stripMargin, e)
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

final class RestApiCommunicationException(msg: String) extends RestApiException(msg)
