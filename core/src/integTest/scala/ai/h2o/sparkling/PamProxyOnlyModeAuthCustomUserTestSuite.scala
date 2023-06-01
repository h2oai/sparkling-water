package ai.h2o.sparkling

import ai.h2o.sparkling.backend.exceptions.RestApiUnauthorisedException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Ignore}
import org.scalatest.junit.JUnitRunner

import java.io.{File, FileWriter}

@RunWith(classOf[JUnitRunner])
@Ignore //still unstable, to be fixed and unignored next release (SW-2779)
class PamProxyOnlyModeAuthCustomUserTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = {
    val tmpFile = File.createTempFile("sparkling-water-", "-pam-login.conf")
    tmpFile.deleteOnExit()
    val writer = new FileWriter(tmpFile);
    val content =
      """pamloginmodule {
        |     de.codedo.jaas.PamLoginModule required
        |     service = common-auth;
        |};
        |""".stripMargin
    writer.write(content)
    writer.flush()
    writer.close()
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.user.name", "root")
      .set("spark.ext.h2o.proxy.login.only", "true")
      .set("spark.ext.h2o.pam.login", "true")
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    sparkSession("local-cluster[2,1,1024]", sparkConf)
  }

  test("Proxy is not accessible because the cluster is owned by a different user") {
    val conf = hc.getConf
    conf.setH2OCluster(hc.flowIp, hc.flowPort)
    conf.setPamLoginDisabled() // Disabling Pam to avoid credentials generation
    conf.setUserName("jenkins")
    conf.setPassword("jenkins")
    val thrown = intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))

    assert(thrown.getMessage.contains("Login name does not match cluster owner name"))
  }

  test("Proxy is not accessible with invalid credentials") {
    val conf = hc.getConf
    conf.setH2OCluster(hc.flowIp, hc.flowPort)
    conf.setPamLoginDisabled() // Disabling Pam to avoid credentials generation
    conf.setUserName("root")
    conf.setPassword("invalid")
    intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))
  }
}
