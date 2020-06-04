package ai.h2o.sparkling.backend

import java.io.{DataOutputStream, File, FileOutputStream}
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.security.SecureRandom

import ai.h2o.sparkling.H2OContext
import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession
import org.spark_project.guava.hash.HashCodes
import py4j.GatewayServer

object SparklingPy4jGateway extends Logging {

  private def createSecret(): String = {
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](128)
    rnd.nextBytes(secretBytes)
    HashCodes.fromBytes(secretBytes).toString
  }

  def main(args: Array[String]) {
    SparkSession.builder.getOrCreate()
    H2OContext.getOrCreate()
    val address = InetAddress.getByName("0.0.0.0")
    val secret = createSecret()
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(0)
      .javaAddress(address)
      .authToken("ahoj")
    val gatewayServer: GatewayServer = builder.build()
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logDebug(s"Started PythonGatewayServer on port $boundPort")
    }
    System.out.println("PORT " + boundPort)

    // Communicate the connection information back to the python process by writing the
    // information in the requested file. This needs to match the read side in java_gateway.py.
    val connectionInfoPath = new File(sys.env("SW_PYTHON_GATEWAY_FILE"))
    val dos = new DataOutputStream(new FileOutputStream(connectionInfoPath))
    dos.writeInt(boundPort)
    val secretBytes = secret.getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()
    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}
