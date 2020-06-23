package ai.h2o.sparkling.backend

import java.net.InetAddress
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec

import ai.h2o.sparkling.H2OContext
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkFiles}
import py4j.GatewayServer

import scala.io.Source
import java.io.ByteArrayInputStream
import java.security.cert.{Certificate, CertificateFactory, X509Certificate}

import javax.xml.bind.DatatypeConverter
import java.security.KeyStore

import javax.net.ServerSocketFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext}

object SparklingGateway extends Logging {

  private def readFile(file: String): String = {
    val source = Source.fromFile(file)
    val content = source.mkString
    source.close()
    content
  }

  private def parseDERFromPEM(data: String, beginDelimiter: String, endDelimiter: String): Array[Byte] = {
    val der = data.split(beginDelimiter)(1).split(endDelimiter)(0)
    DatatypeConverter.parseBase64Binary(der)
  }

  private def getPrivateKey(path: String): RSAPrivateKey = {
    val data = readFile(path)
    val keyBytes = parseDERFromPEM(data, "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----")
    val spec = new PKCS8EncodedKeySpec(keyBytes)
    val factory = KeyFactory.getInstance("RSA")
    factory.generatePrivate(spec).asInstanceOf[RSAPrivateKey]
  }

  private def getCert(path: String): X509Certificate = {
    val data = readFile(path)
    val certBytes = parseDERFromPEM(data, "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----")
    val factory = CertificateFactory.getInstance("X.509")
    factory.generateCertificate(new ByteArrayInputStream(certBytes)).asInstanceOf[X509Certificate]
  }

  private def createJKS(key: RSAPrivateKey, cert: X509Certificate): KeyStore = {
    val keystore = KeyStore.getInstance("JKS")
    keystore.load(null)
    keystore.setCertificateEntry("cert-alias", cert)
    keystore.setKeyEntry("key-alias", key, "pass".toCharArray, Array[Certificate](cert))
    keystore
  }

  private def createServerSocketFactory(): ServerSocketFactory = {
    val kmf = KeyManagerFactory.getInstance("SunX509")
    val privateKey = getPrivateKey("/Users/kuba/devel/repos/sparkling-water/rsakey.pem")
    val cert = getCert("/Users/kuba/Desktop/cert.pem")
    val jks = createJKS(privateKey, cert)
    kmf.init(jks, "pass".toCharArray())
    val km = kmf.getKeyManagers
    val context = SSLContext.getInstance("TLSv1.1")
    context.init(km, null, null)
    context.getServerSocketFactory
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val conf = spark.sparkContext.getConf
    val hc = H2OContext.getOrCreate()
    val address = InetAddress.getByName("0.0.0.0")
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(gatewayPort(conf))
      .javaAddress(address)
      .authToken(readSecret(conf))
      .serverSocketFactory(createServerSocketFactory())
    val gatewayServer: GatewayServer = builder.build()
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logInfo(s"""Running Py4j Gateway on port $boundPort""")
    }
    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (!(spark.sparkContext.isStopped || hc.isStopped())) {
      Thread.sleep(1000)
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }

  private def readSecret(conf: SparkConf): String = {
    val file = SparkFiles.get(gatewaySecretFileName(conf))
    val bufferedSource = Source.fromFile(file)
    val secret = bufferedSource.getLines.mkString
    bufferedSource.close
    secret
  }

  private val PROP_GATEWAY_PORT = ("spark.ext.h2o.py4j.gateway.port", None)
  private val PROP_GATEWAY_SECRET_FILE_NAME = ("spark.ext.h2o.py4j.gateway.secret.file.name", None)

  private def gatewayPort(conf: SparkConf) = {
    val option = conf.getOption(PROP_GATEWAY_PORT._1)
    if (option.isEmpty) {
      throw new RuntimeException(s"Missing $PROP_GATEWAY_PORT")
    } else {
      option.get.toInt
    }
  }
  private def gatewaySecretFileName(conf: SparkConf) = {
    val option = conf.getOption(PROP_GATEWAY_SECRET_FILE_NAME._1)
    if (option.isEmpty) {
      throw new RuntimeException(s"Missing $PROP_GATEWAY_SECRET_FILE_NAME")
    } else {
      option.get
    }
  }
}
