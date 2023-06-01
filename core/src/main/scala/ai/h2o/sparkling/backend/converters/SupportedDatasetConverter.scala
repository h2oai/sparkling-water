package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.expose.Logging

object SupportedDatasetConverter extends Logging {

  def toH2OFrame(hc: H2OContext, dataset: SupportedDataset, frameKeyName: Option[String]): H2OFrame = {
    dataset.toH2OFrame(hc, frameKeyName)
  }
}
