package ai.h2o.sparkling.ml.models

import java.io.{File, FileInputStream, InputStream}

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.spark.SparkFiles

private[models] trait HasBinaryModel {

  private var binaryModelFileName: Option[String] = None

  private[sparkling] def setBinaryModel(model: InputStream): this.type =
    setBinaryModel(model, binaryModelName = "binaryModel")

  private[sparkling] def setBinaryModel(model: InputStream, binaryModelName: String): this.type = {
    val modelFile = SparkSessionUtils.inputStreamToTempFile(model, binaryModelName, ".bin")
    setBinaryModel(modelFile)
    this
  }

  private[sparkling] def setBinaryModel(model: File): this.type = {
    val sparkSession = SparkSessionUtils.active
    binaryModelFileName = Some(model.getName)
    if (getBinaryModel().isDefined && getBinaryModel().get.exists()) {
      // Copy content to a new temp file
      withResource(new FileInputStream(model)) { inputStream =>
        setBinaryModel(inputStream, binaryModelFileName.get)
      }
    } else {
      sparkSession.sparkContext.addFile(model.getAbsolutePath)
    }
    this
  }

  private[sparkling] def getBinaryModel(): Option[File] =
    binaryModelFileName.map(path => new File(SparkFiles.get(path)))
}
