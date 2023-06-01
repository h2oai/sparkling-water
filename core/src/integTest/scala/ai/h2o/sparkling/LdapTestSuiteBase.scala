package ai.h2o.sparkling

import com.unboundid.ldap.listener.{InMemoryDirectoryServer, InMemoryDirectoryServerConfig, InMemoryListenerConfig}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import java.io.{File, FileWriter}

@RunWith(classOf[JUnitRunner])
abstract class LdapTestSuiteBase extends FunSuite with SharedH2OTestContext with Matchers {

  protected val SwClusterOwnerName = "user01"
  protected val SwClusterOwnerPassword = "password1"
  protected val LdapAdmin = "admin"
  protected val LdapAdminPassword = "adminpassword"
  protected val LdapBaseDn = "dc=example,dc=org"
  protected val LdapPort = 1389
  protected val LdapConnectionConfig =
    s"""
       |ldaploginmodule {
       |    ai.h2o.org.eclipse.jetty.plus.jaas.spi.LdapLoginModule required
       |    debug="true"
       |    useLdaps="false"
       |    contextFactory="com.sun.jndi.ldap.LdapCtxFactory"
       |    hostname="localhost"
       |    port="$LdapPort"
       |    bindDn="cn=$LdapAdmin,$LdapBaseDn"
       |    bindPassword="$LdapAdminPassword"
       |    authenticationMethod="simple"
       |    forceBindingLogin="true"
       |    userBaseDn="$LdapBaseDn";
       |};
       |""".stripMargin
  protected var ldap: InMemoryDirectoryServer = _

  override def beforeAll(): Unit = {
    ldap = getInMemoryLdapServer(listenPort = LdapPort)
    ldap.startListening()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ldap.shutDown(true)
    super.afterAll()
  }

  protected def getInMemoryLdapServer(listenPort: Int) = {
    val config = new InMemoryDirectoryServerConfig(LdapBaseDn)
    config.addAdditionalBindCredentials(s"cn=$LdapAdmin,$LdapBaseDn", LdapAdminPassword)
    config.setListenerConfigs(new InMemoryListenerConfig("sw-test-ldap", null, listenPort, null, null, null))
    val inMemoryLdap = new InMemoryDirectoryServer(config)
    inMemoryLdap.add(s"dn: $LdapBaseDn", "objectClass: top", "objectClass: domain", "dc: example")
    inMemoryLdap.add(
      s"dn: uid=$SwClusterOwnerName,$LdapBaseDn",
      "objectClass: top",
      "objectClass: person",
      "objectClass: inetOrgPerson",
      "uid: " + SwClusterOwnerName,
      "givenName: Test",
      "sn: " + SwClusterOwnerName,
      "cn: " + SwClusterOwnerName,
      "userPassword: " + SwClusterOwnerPassword)
    inMemoryLdap
  }

  protected def writeTmpFile(suffix: String, content: String): File = {
    val tmpFile = File.createTempFile("sparkling-water-", suffix)
    tmpFile.deleteOnExit()
    val writer = new FileWriter(tmpFile)
    writer.write(content)
    writer.flush()
    writer.close()
    tmpFile
  }

}
