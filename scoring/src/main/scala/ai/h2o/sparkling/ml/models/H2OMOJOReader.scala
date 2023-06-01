package ai.h2o.sparkling.ml.models

import org.apache.hadoop.fs.Path
import ai.h2o.sparkling.ml.utils.{H2OReaderBase, Utils}
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils._

private[models] class H2OMOJOReader[T <: HasMojo] extends H2OReaderBase[T] {

  override def load(path: String): T = {
    val model = super.load(path)
    val inputPath = new Path(path, H2OMOJOProps.serializedFileName)
    withResource(SparkSessionUtils.readHDFSFile(inputPath)) { inputStream =>
      model.setMojo(inputStream)
    }
    if (model.isInstanceOf[H2OMOJOModel]) {
      val mojoModel = model.asInstanceOf[H2OMOJOModel]
      mojoModel.h2oMojoModel = Utils.getMojoModel(mojoModel.getMojo())
      val numberOfCVModels = mojoModel.getOrDefault(mojoModel.numberOfCrossValidationModels)
      if (numberOfCVModels > 0) {
        val cvModels = (0 until numberOfCVModels).map { i =>
          val cvModelPath = new Path(new Path(path, H2OMOJOProps.crossValidationDirectoryName), i.toString).toString
          load(cvModelPath).asInstanceOf[H2OMOJOModel]
        }.toArray
        mojoModel.setCrossValidationModels(cvModels)
      }
    }
    model
  }
}
