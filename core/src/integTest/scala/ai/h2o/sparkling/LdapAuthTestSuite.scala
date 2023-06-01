package ai.h2o.sparkling

import ai.h2o.sparkling.H2OFrame.query
import ai.h2o.sparkling.backend.exceptions.RestApiUnauthorisedException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.PingV3

import java.io.File
import java.net.URI

@RunWith(classOf[JUnitRunner])
class LdapAuthTestSuite extends LdapTestSuiteBase {

  test("SW internally should communicate with h2o-3 in LDAP without problems") {
    RestApiUtils.getPingInfo(hc.getConf)
  }

  test("H2O endpoint should be accessible with correct LDAP credentials") {
    val conf = hc.getConf
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
    RestApiUtils.getPingInfo(conf)
  }

  test("H2O endpoint should not be accessible with correct LDAP credentials") {
    val conf = hc.getConf
      .setUserName(SwClusterOwnerName)
      .setPassword("WRONG_PASSWORD")
    intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))
  }

  test("Flow proxy should be available with correct LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
    query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf)
  }

  test("Flow proxy should not be available with wrong LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword("WRONG_PASSWORD")
    intercept[RestApiUnauthorisedException](query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf))
  }
  override def createSparkSession(): SparkSession = {
    val tmpFile: File = writeTmpFile("-ldap-login.conf", LdapConnectionConfig)
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.ldap.login", "true")
      .set("spark.ext.h2o.user.name", SwClusterOwnerName)
      .set("spark.ext.h2o.password", SwClusterOwnerPassword)
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    val result = sparkSession("local-cluster[2,1,1024]", sparkConf)
    result.sparkContext.addFile(classOf[LdapAuthTestSuite].getClassLoader.getResource("log4j.properties").getPath)
    result
  }

}
