package org.apache.spark.examples.h2o

import hex.splitframe.ShuffleSplitFrame
import hex.tree.gbm.GBMModel
import org.apache.spark.{SparkConf, SparkContext}
import water.fvec.{Frame, DataFrame, Chunk}
import water.parser.ValueString
import water._

/**
 * Shared demo utility functions.
 */
object DemoUtils {

  def createSparkContext(sparkMaster:String = null, registerH2OExtension: Boolean = true): SparkContext = {
    val h2oWorkers = System.getProperty("spark.h2o.workers", "3") // N+1 workers, one is running in driver
    val h2oCloudTimeout = System.getProperty("spark.ext.h2o.cloud.timeout", "60000").toInt
    //
    // Create application configuration
    //
    val conf = new SparkConf()
        .setAppName("H2O Integration Example")
    if (System.getProperty("spark.master")==null) conf.setMaster(if (sparkMaster==null) "local" else sparkMaster)
    // For local development always wait for cloud of size 1
    conf.set("spark.ext.h2o.cluster.size",
             if (conf.get("spark.master").equals("local") || conf.get("spark.master").startsWith("local["))
               "1" else h2oWorkers)
    //
    // Setup H2O extension of Spark platform proposed by SPARK JIRA-3270
    //
    if (registerH2OExtension) {
      //conf.addExtension[H2OPlatformExtension] // add H2O extension
    }

    val sc = new SparkContext(conf)
    //
    // In non-local case we create a small h2o instance in driver to have access to the c,oud
    //
    if (registerH2OExtension) {
      if (!sc.isLocal) {
        println("Waiting for " + h2oWorkers)
        H2OClientApp.start()
        H2O.waitForCloudSize(h2oWorkers.toInt /* One H2ONode to match the one Spark worker and one is running in driver*/
          , h2oCloudTimeout)
      } else {
        // Since LocalBackend does not wait for initialization (yet)
        H2O.waitForCloudSize(1, h2oCloudTimeout)
      }
    }
    sc
  }

  def configure(appName:String = "Sparkling Water Demo"):SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
    conf
  }

  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach( f => sc.addFile(f) )
  }

  def printFrame(fr: DataFrame): Unit = {
    new MRTask {
      override def map(cs: Array[Chunk]): Unit = {
        println ("Chunks: " + cs.mkString(","))
        for (r <- 0 until cs(0)._len) {
          for (c <- cs) {
            val vstr = new ValueString
            if (c.vec().isString) {
              c.atStr(vstr, r)
              print(vstr.toString + ",")
            } else if (c.vec().isEnum) {
              print(c.vec().domain()(c.at8(r).asInstanceOf[Int]) + ", ")
            } else {
              print(c.atd(r) + ", ")
            }
          }
          println()
        }
      }
    }.doAll(fr)
  }

  def residualPlotRCode(prediction:Frame, predCol: String, actual:Frame, actCol:String):String = {
    s"""# R script for residual plot
        |library(h2o)
        |h = h2o.init()
        |
        |pred = h2o.getFrame(h, "${prediction._key}")
        |act = h2o.getFrame (h, "${actual._key}")
        |
        |predDelay = pred$$${predCol}
        |actDelay = act$$${actCol}
        |
        |nrow(actDelay) == nrow(predDelay)
        |
        |residuals = predDelay - actDelay
        |
        |compare = cbind (as.data.frame(actDelay$$ArrDelay), as.data.frame(residuals$$predict))
        |nrow(compare)
        |plot( compare[,1:2] )
        |
      """.stripMargin
  }

  def splitFrame(df: DataFrame, keys: Seq[String], ratios: Seq[Double]): Array[Frame] = {
    val ks = keys.map(Key.make(_)).toArray
    val frs = ShuffleSplitFrame.shuffleSplitFrame(df, ks, ratios.toArray, 1234567689L)
    frs
  }

  def r2(model: GBMModel, fr: Frame) =  hex.ModelMetrics.getFromDKV(model, fr).asInstanceOf[hex.ModelMetricsSupervised].r2()

  case class R2(name:String, train:Double, test:Double, hold:Double) {
    override def toString: String =
      s"""
        |Results for $name:
        |  - R2 on train = ${train}
        |  - R2 on test  = ${test}
        |  - R2 on hold  = ${hold}
      """.stripMargin
  }
}
