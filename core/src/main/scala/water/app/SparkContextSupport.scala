package water.app

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Publish useful method to configure Spark context.
 */
trait SparkContextSupport {

  def configure(appName:String = "Sparkling Water Demo"):SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
    conf
  }

  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach( f => sc.addFile(f) )
  }

  def absPath(path: String): String = new java.io.File(path).getAbsolutePath
}
