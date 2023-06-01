package ai.h2o.sparkling.ml.models

import java.io.InputStream

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.util.Identifiable

trait H2OMOJOLoader[T] {
  def createFromMojo(path: String): T = createFromMojo(path, H2OMOJOSettings.default)

  def createFromMojo(path: String, settings: H2OMOJOSettings): T = {
    val inputPath = new Path(path)
    createFromMojo(path, Identifiable.randomUID(inputPath.getName), settings)
  }

  def createFromMojo(path: String, uid: String): T = createFromMojo(path, uid, H2OMOJOSettings.default)

  def createFromMojo(path: String, uid: String, settings: H2OMOJOSettings): T = {
    withResource(SparkSessionUtils.readHDFSFile(path)) { inputStream =>
      createFromMojo(inputStream, uid, settings)
    }
  }

  def createFromMojo(inputStream: InputStream, uid: String): T = {
    createFromMojo(inputStream, uid, H2OMOJOSettings.default)
  }

  def createFromMojo(inputStream: InputStream, uid: String, settings: H2OMOJOSettings): T
}
