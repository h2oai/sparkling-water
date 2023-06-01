package ai.h2o.sparkling.backend.utils

import java.io._
import java.net.{HttpURLConnection, URI, URL}
import java.security.KeyStore

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.NodeDesc
import ai.h2o.sparkling.backend.exceptions._
import ai.h2o.sparkling.utils.ScalaUtils._
import ai.h2o.sparkling.utils.{Compression, FinalizingOutputStream}
import com.google.gson.{ExclusionStrategy, FieldAttributes, Gson, GsonBuilder, JsonElement, JsonObject}
import javax.net.ssl._
import java.security.cert.X509Certificate
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkFiles
import org.apache.spark.expose.Logging

import scala.collection.immutable.Map
import scala.reflect.{ClassTag, classTag}

trait RestCommunication extends Logging with RestEncodingUtils {

  object LoggingLevel extends Enumeration {
    type LoggingLevel = Value
    val Info, Debug = Value
  }

  import LoggingLevel._

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
      skippedFields: Seq[(Class[_], String)] = Seq.empty,
      confirmationLoggingLevel: LoggingLevel = Info): ResultType = {
    val encodeParamsAsJson = false
    request(endpoint, "GET", suffix, conf, params, skippedFields, encodeParamsAsJson, confirmationLoggingLevel)
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
      skippedFields: Seq[(Class[_], String)] = Seq.empty,
      encodeParamsAsJson: Boolean = false): ResultType = {
    request(endpoint, "POST", suffix, conf, params, skippedFields, encodeParamsAsJson)
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
  def delete[ResultType: ClassTag](
      endpoint: URI,
      suffix: String,
      conf: H2OConf,
      params: Map[String, Any] = Map.empty,
      skippedFields: Seq[(Class[_], String)] = Seq.empty,
      encodeParamsAsJson: Boolean = false): ResultType = {
    request(endpoint, "DELETE", suffix, conf, params, skippedFields, encodeParamsAsJson)
  }

  /**
    *
    * @param node   H2O node descriptor
    * @param suffix REST relative path representing a specific call
    * @param conf   H2O conf object
    * @param params Query parameters
    * @return HttpUrlConnection facilitating the insertion and holding the outputStream
    */
  def insertToNode(
      node: NodeDesc,
      suffix: String,
      conf: H2OConf,
      params: Map[String, Any] = Map.empty): OutputStream = {
    val endpoint = RestApiUtils.resolveNodeEndpoint(node, conf)
    val addCompression =
      (outputStream: OutputStream) => Compression.compress(conf.externalCommunicationCompression, outputStream)
    insert(endpoint, suffix, conf, addCompression, params)
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
      val connection = openUrlConnection(url, conf)
      val requestMethod = "PUT"
      connection.setRequestMethod(requestMethod)
      connection.setDoOutput(true)
      connection.setChunkedStreamingMode(-1) // -1 to use default size
      setHeaders(connection, conf, requestMethod, params, encodeParamsAsJson = false, None)
      val outputStream = connection.getOutputStream()
      val wrappedStream = streamWrapper(outputStream)
      new FinalizingOutputStream(wrappedStream, () => checkResponseCode(connection))
    } catch {
      case e: Exception => throwRestApiNotReachableException(url, e)
    }
  }

  /**
    *
    * @param endpoint An address of H2O node with exposed REST endpoint
    * @param suffix   REST relative path representing a specific call
    * @param conf     H2O conf object
    */
  protected def delete(endpoint: URI, suffix: String, conf: H2OConf): Unit = {
    withResource(readURLContent(endpoint, "DELETE", suffix, conf, Map.empty, encodeParamsAsJson = false, None))(
      identity)
  }

  def request[ResultType: ClassTag](
      endpoint: URI,
      requestType: String,
      suffix: String,
      conf: H2OConf,
      params: Map[String, Any] = Map.empty,
      skippedFields: Seq[(Class[_], String)] = Seq.empty,
      encodeParamsAsJson: Boolean = false,
      confirmationLoggingLevel: LoggingLevel = Info): ResultType = {
    withResource(
      readURLContent(endpoint, requestType, suffix, conf, params, encodeParamsAsJson, None, confirmationLoggingLevel)) {
      response =>
        val content = IOUtils.toString(response)
        deserialize[ResultType](content, skippedFields)
    }
  }

  private def createGsonSerde(skippedFields: Seq[(Class[_], String)]): Gson = {
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
    builder.create()
  }

  private[sparkling] def deserialize[ResultType: ClassTag](
      content: String,
      skippedFields: Seq[(Class[_], String)]): ResultType = {
    createGsonSerde(skippedFields).fromJson(content, classTag[ResultType].runtimeClass)
  }

  private[sparkling] def deserialize[ResultType: ClassTag](
      content: JsonElement,
      skippedFields: Seq[(Class[_], String)]): ResultType = {
    createGsonSerde(skippedFields).fromJson(content, classTag[ResultType].runtimeClass)
  }

  private[sparkling] def deserializeAsJsonObject(
      content: String,
      skippedFields: Seq[(Class[_], String)]): JsonObject = {
    createGsonSerde(skippedFields).fromJson(content, classOf[JsonObject])
  }

  protected def downloadBinaryURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
    withResource(readURLContent(endpoint, "GET", suffix, conf, Map.empty, encodeParamsAsJson = false, None)) { input =>
      withResource(new BufferedOutputStream(new FileOutputStream(file))) { output =>
        IOUtils.copy(input, output)
      }
    }
  }

  protected def downloadStringURLContent(endpoint: URI, suffix: String, conf: H2OConf, file: File): Unit = {
    withResource(readURLContent(endpoint, "GET", suffix, conf, Map.empty, encodeParamsAsJson = false, None)) { input =>
      withResource(new java.io.FileWriter(file)) { output =>
        IOUtils.copy(input, output)
      }
    }
  }

  private def urlToString(url: URL) = s"${url.getProtocol}://${url.getHost}:${url.getPort}"

  private def openUrlConnection(url: URL, conf: H2OConf): HttpURLConnection = {
    val connection = url.openConnection()
    if (connection.isInstanceOf[HttpsURLConnection]) {
      val secureConnection = connection.asInstanceOf[HttpsURLConnection]

      if (!conf.isSslCertificateVerificationInInternalRestConnectionsEnabled) {
        disableCertificateVerification(secureConnection)
      } else if (conf.autoFlowSsl) {
        setSelfSignedCertificateVerification(secureConnection, conf)
      }

      if (!conf.isSslHostnameVerificationInInternalRestConnectionsEnabled ||
          !conf.isSslCertificateVerificationInInternalRestConnectionsEnabled ||
          conf.autoFlowSsl) {
        disableHostnameVerification(secureConnection)
      }

      secureConnection
    } else {
      connection.asInstanceOf[HttpURLConnection]
    }
  }

  private def createSSLContext(): SSLContext = SSLContext.getInstance("TLSv1.2")

  private def disableHostnameVerification(secureConnection: HttpsURLConnection): Unit = {
    val hostnameVerifier = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }
    secureConnection.setHostnameVerifier(hostnameVerifier)
  }

  private def disableCertificateVerification(connection: HttpsURLConnection): Unit = {
    val allCertificatesTrustedManager = new X509TrustManager() {
      def getAcceptedIssuers: Array[X509Certificate] = null
      def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
      def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    }
    val sslContext = createSSLContext()
    sslContext.init(null, Array(allCertificatesTrustedManager), null)
    connection.setSSLSocketFactory(sslContext.getSocketFactory)
  }

  private def setSelfSignedCertificateVerification(connection: HttpsURLConnection, conf: H2OConf): Unit = {
    val keyStore = loadKeyStore(conf)

    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(keyStore)

    val sslContext = createSSLContext()
    sslContext.init(null, trustManagerFactory.getTrustManagers, null)
    connection.setSSLSocketFactory(sslContext.getSocketFactory)
  }

  private def loadKeyStore(conf: H2OConf): KeyStore = {
    val localJKSFile = SparkFiles.get(new File(conf.jks.get).getName)
    withResource(new FileInputStream(localJKSFile)) { inputStream =>
      val keyStore = KeyStore.getInstance("JKS")
      val password = conf.jksPass.get
      keyStore.load(inputStream, password.toCharArray)
      keyStore
    }
  }

  private def resolveUrl(endpoint: URI, suffix: String): URL = {
    val endpointAsString = endpoint.toString
    val endpointWithDelimiter = if (endpointAsString.endsWith("/")) endpointAsString else endpointAsString + "/"
    val suffixWithoutDelimiter = suffix.stripPrefix("/")
    val handler = if (endpoint.getScheme == "https") {
      new sun.net.www.protocol.https.Handler()
    } else {
      new sun.net.www.protocol.http.Handler()
    }
    val spec = new URI(endpointWithDelimiter).resolve(suffixWithoutDelimiter).toURL.toString
    new URL(null, spec, handler)
  }

  private def setHeaders(
      connection: HttpURLConnection,
      conf: H2OConf,
      requestType: String,
      params: Map[String, Any],
      encodeParamsAsJson: Boolean = false,
      file: Option[String]): Unit = {
    conf.getCredentials().foreach(c => connection.setRequestProperty("Authorization", c.toBasicAuth))

    if (params.nonEmpty && file.isEmpty && requestType == "POST") {
      if (encodeParamsAsJson) {
        connection.setRequestProperty("Content-Type", "application/json")
      } else {
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      }
      val paramsAsBytes = stringifyParams(params, encodeParamsAsJson).getBytes("UTF-8")
      connection.setRequestProperty("charset", "UTF-8")
      connection.setRequestProperty("Content-Length", Integer.toString(paramsAsBytes.length))
      connection.setDoOutput(true)
      withResource(new DataOutputStream(connection.getOutputStream())) { writer =>
        writer.write(paramsAsBytes)
      }
    }
    if (params.isEmpty && file.isDefined && requestType == "POST") {
      val boundary = s"===${System.currentTimeMillis}==="
      connection.setRequestProperty("Content-Type", s"multipart/form-data;boundary=$boundary")
      connection.setRequestProperty("charset", "UTF-8")
      val CRLF = "\r\n"
      connection.setDoOutput(true)
      val body = new PrintWriter(new OutputStreamWriter(connection.getOutputStream, "UTF-8"), true)
      body.append(CRLF)
      body.flush()

      IOUtils.copy(new FileInputStream(file.get), connection.getOutputStream)
      connection.getOutputStream.flush()
      body.append(CRLF)
      body.flush()
      body.append("--").append(boundary).append("--").append(CRLF)
      body.flush()
    }
  }

  protected def readURLContent(
      endpoint: URI,
      requestType: String,
      suffix: String,
      conf: H2OConf,
      params: Map[String, Any] = Map.empty,
      encodeParamsAsJson: Boolean = false,
      file: Option[String] = None,
      confirmationLoggingLevel: LoggingLevel = Info): InputStream = {
    val suffixWithParams =
      if (params.nonEmpty && (requestType == "GET")) s"$suffix?${stringifyParams(params)}" else suffix
    val url = resolveUrl(endpoint, suffixWithParams)
    try {
      val connection = openUrlConnection(url, conf)
      connection.setRequestMethod(requestType)
      setHeaders(connection, conf, requestType, params, encodeParamsAsJson, file)
      checkResponseCode(connection, confirmationLoggingLevel)
      connection.getInputStream()
    } catch {
      case e: RestApiException => throw e
      case cause: Exception => throwRestApiNotReachableException(url, cause)
    }
  }

  def checkResponseCode(connection: HttpURLConnection, confirmationLoggingLevel: LoggingLevel = Info): Unit = {
    val url = connection.getURL
    val requestType = connection.getRequestMethod
    val statusCode = retry(3) {
      connection.getResponseCode()
    }
    statusCode match {
      case HttpURLConnection.HTTP_OK =>
        val message = s"H2O node $url successfully responded for the $requestType."
        confirmationLoggingLevel match {
          case Info => logInfo(message)
          case Debug => logDebug(message)
        }
      case HttpURLConnection.HTTP_UNAUTHORIZED =>
        throw new RestApiUnauthorisedException(
          s"""H2O node ${urlToString(url)} could not be reached because the client is not authorized.
           |Please make sure you have passed valid credentials to the client.
           |Status code $statusCode : ${connection.getResponseMessage()}.
           |Server error: ${getServerError(connection)}""".stripMargin)
      case _ => throw new RestApiCommunicationException(s"""H2O node ${urlToString(url)} responded with
           |Status code: $statusCode : ${connection.getResponseMessage()}
           |Server error: ${getServerError(connection)}""".stripMargin)
    }
  }

  private def getServerError(connection: HttpURLConnection): String = {
    if (connection.getErrorStream == null) {
      "No error"
    } else {
      withResource(connection.getErrorStream) { errorStream =>
        IOUtils.toString(errorStream)
      }
    }
  }

  private def throwRestApiNotReachableException(url: URL, e: Exception) = {
    throw new RestApiNotReachableException(
      s"""H2O node ${urlToString(url)} is not reachable.
         |Please verify that you are passing ip and port of existing cluster node and the cluster
         |is running with web enabled.""".stripMargin,
      e)
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
