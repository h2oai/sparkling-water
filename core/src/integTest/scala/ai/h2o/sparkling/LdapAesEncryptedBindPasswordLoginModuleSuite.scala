package ai.h2o.sparkling

import ai.h2o.sparkling.H2OFrame.query
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.PingV3

import java.io.File
import java.net.URI

@RunWith(classOf[JUnitRunner])
class LdapAesEncryptedBindPasswordLoginModuleSuite extends LdapTestSuiteBase {

  private val EncryptedLdapAdminBindPassword = "HjcJOYaTyhANxDx0xRaZ6Q=="
  private val TestAesKey = "b38b730d4cc721156e3760d1d58546ce697adc939188e4c6a80f0e24e032b9b7"
  private val TestAesIV = "064df9633d9f5dd0b5614843f6b4b059"

  private val LdapAesConnectionConfig =
    s"""
       |ldaploginmodule {
       |    water.webserver.jetty9.LdapAesEncryptedBindPasswordLoginModule required
       |    debug="true"
       |    useLdaps="false"
       |    contextFactory="com.sun.jndi.ldap.LdapCtxFactory"
       |    hostname="localhost"
       |    port="$LdapPort"
       |    bindDn="cn=$LdapAdmin,$LdapBaseDn"
       |    encryptedBindPassword="$EncryptedLdapAdminBindPassword"
       |    authenticationMethod="simple"
       |    forceBindingLogin="true"
       |    userBaseDn="$LdapBaseDn";
       |};
       |""".stripMargin

  test("LdapAesCbcEncryptedPasswordLoginModule should allow the binding") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf)
  }

  override def createSparkSession(): SparkSession = {
    val tmpFile: File = writeTmpFile("-ldap-login.conf", LdapAesConnectionConfig)
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.ldap.login", "true")
      .set("spark.ext.h2o.proxy.login.only", "true")
      .set("spark.ext.h2o.jetty.aes.login.module.key", TestAesKey)
      .set("spark.ext.h2o.jetty.aes.login.module.iv", TestAesIV)
      .set("spark.ext.h2o.user.name", SwClusterOwnerName)
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    val result = sparkSession("local-cluster[2,1,1024]", sparkConf)
    result.sparkContext.addFile(
      classOf[LdapAesEncryptedBindPasswordLoginModuleSuite].getClassLoader.getResource("log4j.properties").getPath)
    result
  }

}
