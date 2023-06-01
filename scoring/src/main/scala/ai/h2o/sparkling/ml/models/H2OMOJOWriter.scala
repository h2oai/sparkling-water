package ai.h2o.sparkling.ml.models

import java.io.File
import java.nio.file.Files

import ai.h2o.sparkling.utils.ScalaUtils._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.ml.util.expose.DefaultParamsWriter

private[models] class H2OMOJOWriter(instance: Params, val mojoPath: String) extends MLWriter {

  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)

    val outputPath = new Path(path, H2OMOJOProps.serializedFileName)
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    withResource(fs.create(qualifiedOutputPath)) { out =>
      Files.copy(new File(mojoPath).toPath, out)
    }
    if (instance.isInstanceOf[H2OMOJOModel]) {
      val cvModels = instance.asInstanceOf[H2OMOJOModel].getCrossValidationModels()
      if (cvModels != null) {
        val cvPath = new Path(path, H2OMOJOProps.crossValidationDirectoryName)
        for (i <- cvModels.indices) {
          val cvModelPath = new Path(cvPath, i.toString)
          val writer = new H2OMOJOWriter(cvModels(i), cvModels(i).getMojo().getAbsolutePath)
          writer.saveImpl(cvModelPath.toString)
        }
      }
    }

    logInfo(s"Saved to: $qualifiedOutputPath")
  }

}
