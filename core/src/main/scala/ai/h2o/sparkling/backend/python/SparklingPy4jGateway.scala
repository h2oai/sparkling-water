package ai.h2o.sparkling.backend.python

import java.net.InetAddress

import ai.h2o.sparkling.H2OContext
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkFiles}
import py4j.GatewayServer

import scala.io.Source

object SparklingPy4jGateway extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val conf = spark.sparkContext.getConf
    H2OContext.getOrCreate()
    val address = InetAddress.getByName("0.0.0.0")
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(gatewayPort(conf))
      .javaAddress(address)
      .authToken(readSecret(conf))
    val gatewayServer: GatewayServer = builder.build()
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logInfo(s"Running Py4j Gateway on port ${boundPort}")
    }
    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (!spark.sparkContext.isStopped) {
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
