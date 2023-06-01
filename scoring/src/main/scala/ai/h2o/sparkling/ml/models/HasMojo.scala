package ai.h2o.sparkling.ml.models

import java.io.{File, FileInputStream, InputStream}

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.spark.SparkFiles

private[models] trait HasMojo {

  private[sparkling] var mojoFileName: String = _

  def setMojo(mojo: InputStream): this.type = setMojo(mojo, mojoName = "mojoData")

  def setMojo(mojo: InputStream, mojoName: String): this.type = {
    val mojoFile = SparkSessionUtils.inputStreamToTempFile(mojo, mojoName, ".mojo")
    setMojo(mojoFile)
    this
  }

  def setMojo(mojo: File): this.type = {
    val sparkSession = SparkSessionUtils.active
    mojoFileName = mojo.getName
    if (getMojo().exists()) {
      // Copy content to a new temp file
      withResource(new FileInputStream(mojo)) { inputStream =>
        setMojo(inputStream, mojoFileName)
      }
    } else {
      sparkSession.sparkContext.addFile(mojo.getAbsolutePath)
    }
    this
  }

  private[sparkling] def getMojo(): File = new File(SparkFiles.get(mojoFileName))
}
