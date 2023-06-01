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
class LdapProxyOnlyModeAuthTestSuite extends LdapTestSuiteBase {

  test("SW internally should communicate with h2o-3 in LDAP Proxy only mode without problems") {
    RestApiUtils.getPingInfo(hc.getConf)
  }

  test("H2O endpoint should not be accessible even with correct LDAP credentials") {
    val conf = hc.getConf
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))
  }

  test("Flow proxy should be available with correct LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf)
  }

  test("Flow proxy should not be available with wrong LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword("WRONG_PASSWORD")
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    intercept[RestApiUnauthorisedException](query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf))
  }

  override def createSparkSession(): SparkSession = {
    val tmpFile: File = writeTmpFile("-ldap-login.conf", LdapConnectionConfig)
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.proxy.login.only", "true")
      .set("spark.ext.h2o.ldap.login", "true")
      .set("spark.ext.h2o.user.name", SwClusterOwnerName)
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    val result = sparkSession("local-cluster[2,1,1024]", sparkConf)
    result.sparkContext.addFile(
      classOf[LdapProxyOnlyModeAuthTestSuite].getClassLoader.getResource("log4j.properties").getPath)
    result
  }

}
