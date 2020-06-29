package ai.h2o.sparkling.backend

import java.io.FileInputStream
import java.net.InetAddress
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.net.ServerSocketFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkFiles}
import py4j.GatewayServer

import scala.io.Source
import scala.util.Random

object SparklingGateway extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val conf = spark.sparkContext.getConf
    val secret = readSecret(conf)
    val address = InetAddress.getByName("0.0.0.0")
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(gatewayPort(conf))
      .javaAddress(address)
      .authToken(secret)
      .serverSocketFactory(createServerSocketFactory(conf, secret))
    val gatewayServer: GatewayServer = builder.build()
    gatewayServer.start()
    val boundPort = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logInfo(s"""Running Py4j Gateway on port $boundPort""")
    }
    conf.set(PROP_PY4J_GATEWAY_PORT._1, boundPort.toString)
    val hc = H2OContext.getOrCreate(new H2OConf(conf))

    while (!(spark.sparkContext.isStopped || hc.isStopped())) {
      Thread.sleep(1000)
    }
    logDebug("Exiting since we received request to stop Spark or H2O.")
    System.exit(0)
  }

  private def readSecret(conf: SparkConf): String = {
    val file = gatewaySecretFileName(conf)
    val bufferedSource = Source.fromFile(file)
    val secret = bufferedSource.getLines.mkString
    bufferedSource.close
    secret
  }

  /** Port for the Python Gateway. 0 means automatic selection */
  private val PROP_PY4J_GATEWAY_PORT: (String, Int) = ("spark.ext.h2o.py4j.gateway.port", 0)
  private val PROP_GATEWAY_SECRET_FILE_NAME = ("spark.ext.h2o.py4j.gateway.secret.file.name", None)
  // Sparkling Water expect keystore in a format of PKCS12 containing private key encoded as PKCS8 and certificate
  // Also we expect that the PKCS12 keystore is not protected by a password
  private val PROP_GATEWAY_KEYSTORE_FILE_NAME = ("spark.ext.h2o.py4j.gateway.keystore.file.name", None)
  private val DUMMY_JKS_PASSWORD = Random.alphanumeric.take(20).mkString("").toCharArray

  private def gatewayPort(conf: SparkConf): Int = {
    conf.getInt(PROP_PY4J_GATEWAY_PORT._1, PROP_PY4J_GATEWAY_PORT._2)
  }

  private def gatewaySecretFileName(conf: SparkConf): String = {
    SparkFiles.get(getOption(conf, PROP_GATEWAY_SECRET_FILE_NAME._1))
  }

  private def pkcs12KeyStoreFileName(conf: SparkConf): String = {
    SparkFiles.get(getOption(conf, PROP_GATEWAY_KEYSTORE_FILE_NAME._1))
  }

  private def getOption(conf: SparkConf, optionName: String): String = {
    val option = conf.getOption(optionName)
    if (option.isEmpty) {
      throw new RuntimeException(s"Missing ${optionName}")
    } else {
      option.get
    }
  }

  private def loadInputKeyStore(conf: SparkConf, secret: String): KeyStore = {
    val stream = new FileInputStream(pkcs12KeyStoreFileName(conf))
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(stream, secret.toCharArray)
    ks
  }

  private def createJKS(key: PrivateKey, cert: Certificate): KeyStore = {
    val keystore = KeyStore.getInstance("JKS")
    keystore.load(null)
    keystore.setCertificateEntry("cert-alias", cert)
    keystore.setKeyEntry("key-alias", key, DUMMY_JKS_PASSWORD, Array(cert))
    keystore
  }

  private def createServerSocketFactory(conf: SparkConf, secret: String): ServerSocketFactory = {
    val inputKeyStore = loadInputKeyStore(conf, secret)
    val kmf = KeyManagerFactory.getInstance("SunX509")
    val privateKey =
      inputKeyStore.getKey(inputKeyStore.aliases().nextElement(), secret.toCharArray).asInstanceOf[PrivateKey]
    val cert = inputKeyStore.getCertificate(inputKeyStore.aliases().nextElement())
    val jks = createJKS(privateKey, cert)
    kmf.init(jks, DUMMY_JKS_PASSWORD)
    val km = kmf.getKeyManagers
    val context = SSLContext.getInstance("TLSv1.2")
    context.init(km, null, null)
    context.getServerSocketFactory
  }
}
