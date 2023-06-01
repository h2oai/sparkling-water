package ai.h2o.sparkling

import java.io.{File, IOException, PrintWriter}
import java.net.URI

import ai.h2o.sparkling.backend.api.dataframes.DataFrames
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.CloudV3

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class H2OContextAuthTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession =
    sparkSession(
      "local[*]",
      defaultSparkConf
        .set("spark.ext.h2o.login.conf", createLoginConf())
        .set("spark.ext.h2o.hash.login", "true")
        .set("spark.ext.h2o.user.name", "admin")
        .set("spark.ext.h2o.password", "admin_password"))

  private def createLoginConf(): String = {
    val file = File.createTempFile("login", ".conf")
    file.deleteOnExit()
    val filePath = file.getAbsolutePath
    new PrintWriter(filePath) { write("admin:admin_password"); close() }
    filePath
  }

  test("H2O endpoint behind Flow Proxy is accessible with provided username and password") {
    val cloudV3 = RestApiUtils.query[CloudV3](new URI(s"http://${hc.flowIp}:${hc.flowPort}"), "3/Cloud", hc.getConf)
    assert(cloudV3 != null)
    assert(cloudV3.cloud_name == hc.getConf.cloudName.get)
  }

  test("SW endpoint behind Flow Proxy is accessible with provided username and password") {
    import spark.implicits._
    spark.sparkContext.parallelize(1 to 10).toDF().createOrReplaceTempView("table_name")
    val dataFrames =
      RestApiUtils.query[DataFrames](new URI(s"http://${hc.flowIp}:${hc.flowPort}"), "3/dataframes", hc.getConf)
    assert(dataFrames != null)
    assert(dataFrames.dataframes.head.dataframe_id == "table_name")
  }

  test("H2O endpoint behind Flow Proxy is not accessible without provided username and password") {
    val url = s"http://${hc.flowIp}:${hc.flowPort}/3/Cloud"
    val thrown = intercept[IOException] {
      Source.fromURL(url)
    }
    assert(thrown.getMessage == s"Server returned HTTP response code: 401 for URL: $url")
  }

  test("SW endpoint behind Flow Proxy is not accessible without provided username and password") {
    val url = s"http://${hc.flowIp}:${hc.flowPort}/3/dataframes"
    val thrown = intercept[IOException] {
      Source.fromURL(url)
    }
    assert(thrown.getMessage == s"Server returned HTTP response code: 401 for URL: $url")
  }
}
