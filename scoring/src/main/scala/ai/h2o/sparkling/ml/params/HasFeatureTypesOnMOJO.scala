package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.Params

trait HasFeatureTypesOnMOJO extends Params {
  protected final val featureTypes: MapStringStringParam =
    new MapStringStringParam(this, "featureTypes", "Types of feature columns expected by the model")

  def getFeatureTypes(): Map[String, String] = $(featureTypes)

  setDefault(featureTypes -> Map.empty[String, String])
}
