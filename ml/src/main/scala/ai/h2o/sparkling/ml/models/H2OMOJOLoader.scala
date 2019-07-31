package ai.h2o.sparkling.ml.models

import java.io.InputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.h2o.models.H2OMOJOSettings
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession

trait H2OMOJOLoader[T] {

  def createFromMojo(path: String): T = createFromMojo(path, H2OMOJOSettings.default)

  def createFromMojo(path: String, settings: H2OMOJOSettings): T = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, Identifiable.randomUID(inputPath.getName), settings)
  }

  def createFromMojo(is: InputStream, uid: String): T = createFromMojo(is, uid, H2OMOJOSettings.default)

  def createFromMojo(is: InputStream, uid: String, settings: H2OMOJOSettings): T = {
    createFromMojo(Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray, uid, settings)
  }

  def createFromMojo(mojoData: Array[Byte], uid: String): T = createFromMojo(mojoData, uid, H2OMOJOSettings.default)

  def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): T
}
