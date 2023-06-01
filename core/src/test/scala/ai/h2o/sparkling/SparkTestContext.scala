package ai.h2o.sparkling

import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import water.init.NetworkInit

/**
  * Helper trait to simplify initialization and termination of Spark contexts.
  */
trait SparkTestContext extends BeforeAndAfterAll {
  self: Suite =>

  def sparkSession(master: String, conf: SparkConf): SparkSession = {
    SparkSession.builder().config(conf).master(master).appName(getClass.getName).getOrCreate()
  }

  def sparkSession(master: String): SparkSession = {
    sparkSession(master, defaultSparkConf)
  }

  def createSparkSession(): SparkSession

  @transient private var sparkInternal: SparkSession = _
  @transient lazy val spark: SparkSession = sparkInternal
  @transient lazy val sc: SparkContext = spark.sparkContext

  override def beforeAll() {
    sparkInternal = createSparkSession()
    System.setProperty("spark.testing", "true")
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
    super.beforeAll()
  }

  def defaultSparkConf: SparkConf = {
    val clusterMode = sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal")
    val clientIp = sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress)
    H2OConf.checkSparkConf({
      val conf = new SparkConf()
        .set("spark.ext.h2o.cloud.name", getClass.getSimpleName)
        .set("spark.driver.memory", "1G")
        .set("spark.executor.memory", "1G")
        .set("spark.app.id", getClass.getSimpleName)
        .set("spark.ext.h2o.log.level", "WARN")
        .set("spark.ext.h2o.repl.enabled", "false") // disable repl in tests
        .set("spark.scheduler.minRegisteredResourcesRatio", "1")
        .set("spark.ext.h2o.backend.cluster.mode", clusterMode)
        .set("spark.ext.h2o.client.ip", clientIp)
        .set("spark.ext.h2o.external.start.mode", "auto")
        .set("spark.ext.h2o.external.memory", "1G")
        .set("spark.ext.h2o.nthreads", "4")
      conf
    })
  }
}
