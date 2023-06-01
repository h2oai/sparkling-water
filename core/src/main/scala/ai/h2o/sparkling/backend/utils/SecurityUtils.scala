package ai.h2o.sparkling.backend.utils

import java.io.File

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkEnv
import org.apache.spark.expose.Utils
import water.network.SecurityUtils.SSLCredentials
import water.network.{SecurityUtils => H2OSecurityUtils}

private[backend] object SecurityUtils {

  def enableSSL(conf: H2OConf): Unit = {
    val spark = SparkSessionUtils.active
    val sslPair = generateSSLPair()
    val config = generateSSLConfig(sslPair)
    conf.set(SharedBackendConf.PROP_SSL_CONF._1, config)
    spark.sparkContext.addFile(sslPair.jks.getLocation)
    if (sslPair.jks.getLocation != sslPair.jts.getLocation) {
      spark.sparkContext.addFile(sslPair.jts.getLocation)
    }
  }

  def enableFlowSSL(conf: H2OConf): H2OConf = {
    val sslPair = generateSSLPair("h2o-internal-auto-flow-ssl")
    conf.setJks(sslPair.jks.getLocation)
    conf.setJksPass(sslPair.jks.pass)
  }

  private def generateSSLPair(): SSLCredentials = generateSSLPair(namePrefix = "h2o-internal")

  private def generateSSLConfig(credentials: SSLCredentials): String = {
    H2OSecurityUtils.generateSSLConfig(credentials)
  }

  private def generateSSLPair(namePrefix: String): SSLCredentials = {
    val nanoTime = System.nanoTime
    val tmpDir = new File(Utils.getLocalDir(SparkEnv.get.conf))
    val name = s"$namePrefix-$nanoTime.jks"
    H2OSecurityUtils.generateSSLPair(
      H2OSecurityUtils.passwordGenerator(16),
      name,
      tmpDir.toPath.toAbsolutePath.toString)
  }
}
