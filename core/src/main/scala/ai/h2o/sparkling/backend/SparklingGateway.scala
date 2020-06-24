package ai.h2o.sparkling.backend

import java.io.FileInputStream
import java.net.InetAddress
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

import ai.h2o.sparkling.H2OContext
import javax.net.ServerSocketFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkFiles}
import py4j.GatewayServer

import scala.io.Source

object SparklingGateway extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val conf = spark.sparkContext.getConf
    val hc = H2OContext.getOrCreate()
    val address = InetAddress.getByName("0.0.0.0")
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(gatewayPort(conf))
      .javaAddress(address)
      .authToken(readSecret(conf))
      .serverSocketFactory(createServerSocketFactory(conf))
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
    val file = gatewaySecretFileName(conf)
    val bufferedSource = Source.fromFile(file)
    val secret = bufferedSource.getLines.mkString
    bufferedSource.close
    secret
  }

  private val PROP_GATEWAY_PORT = ("spark.ext.h2o.py4j.gateway.port", None)
  private val PROP_GATEWAY_SECRET_FILE_NAME = ("spark.ext.h2o.py4j.gateway.secret.file.name", None)
  // Sparkling Water expect keystore in a format of PKCS12 containing private key encoded as PKCS8 and certificate
  // Also we expect that the PKCS12 keystore is not protected by a password
  private val PROP_GATEWAY_KEYSTORE_FILE_NAME = ("spark.ext.h2o.py4j.gateway.keystore.file.name", None)

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
      SparkFiles.get(option.get)
    }
  }

  private def pkcs12KeyStoreFileName(conf: SparkConf) = {
    val option = conf.getOption(PROP_GATEWAY_KEYSTORE_FILE_NAME._1)
    if (option.isEmpty) {
      throw new RuntimeException(s"Missing $PROP_GATEWAY_KEYSTORE_FILE_NAME")
    } else {
      SparkFiles.get(option.get)
    }
  }

  private def loadInputKeyStore(conf: SparkConf): KeyStore = {
    val stream = new FileInputStream(pkcs12KeyStoreFileName(conf))
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(stream, null)
    ks
  }

  private def createJKS(key: PrivateKey, cert: Certificate): KeyStore = {
    val keystore = KeyStore.getInstance("JKS")
    keystore.load(null)
    keystore.setCertificateEntry("cert-alias", cert)
    keystore.setKeyEntry("key-alias", key, "pass".toCharArray, Array(cert))
    keystore
  }

  private def createServerSocketFactory(conf: SparkConf): ServerSocketFactory = {
    val inputKeyStore = loadInputKeyStore(conf)
    val kmf = KeyManagerFactory.getInstance("SunX509")
    val privateKey = inputKeyStore.getKey(inputKeyStore.aliases().nextElement(), null).asInstanceOf[PrivateKey]
    val cert = inputKeyStore.getCertificate(inputKeyStore.aliases().nextElement())
    val jks = createJKS(privateKey, cert)
    kmf.init(jks, "pass".toCharArray())
    val km = kmf.getKeyManagers
    val context = SSLContext.getInstance("TLSv1.2")
    context.init(km, null, null)
    context.getServerSocketFactory
  }
}
