package ai.h2o.sparkling.backend

import java.net.InetAddress

import ai.h2o.sparkling.SparklingWaterDriver.wait
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OConf, H2OContext}
import org.apache.spark.SparkConf
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import py4j.GatewayServer

object SimpleSparklingGateway extends Logging {
  def main(args: Array[String]) {
    val conf: SparkConf = H2OConf.checkSparkConf(
      new SparkConf()
        .setAppName("Simple Sparkling Water P4j gateway")
        .set("spark.ext.h2o.repl.enabled", "true"))

    SparkSessionUtils.createSparkSession(conf)


    val spark = SparkSession.builder.config(conf).getOrCreate()
    val address = InetAddress.getByName("0.0.0.0")
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(55522)
      .javaAddress(address)
      .authToken("dummySecret")
    val gatewayServer: GatewayServer = builder.build()
    gatewayServer.start()
    val boundPort = gatewayServer.getListeningPort
    if (boundPort == -1) {
      println("Error: GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      println(s"""Running Py4j Gateway on port $boundPort""")
    }

    val hc = H2OContext.getOrCreate(new H2OConf(conf))

    println(hc)

    // Infinite wait
    this.synchronized(while (true) {
      wait()
    })
  }
}
