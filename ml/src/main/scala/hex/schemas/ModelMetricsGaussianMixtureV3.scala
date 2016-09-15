package hex.schemas

import org.apache.spark.ml.spark.models.gm.ModelMetricsGaussianMixture
import water.api.API
import water.api.schemas3.ModelMetricsBaseV3

class ModelMetricsGaussianMixtureV3 extends ModelMetricsBaseV3[ModelMetricsGaussianMixture, ModelMetricsGaussianMixtureV3] {
  @API(help = "Log likelihood of the model")
  var loglikelihood: Double = _

}