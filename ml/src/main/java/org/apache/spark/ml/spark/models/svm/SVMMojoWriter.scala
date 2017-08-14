package org.apache.spark.ml.spark.models.svm

import hex.ModelMojoWriter
import org.apache.spark.ml.spark.models.MissingValuesHandling

class SVMMojoWriter(svmModel: SVMModel) extends ModelMojoWriter[SVMModel, SVMParameters, SVMModel.SVMOutput](svmModel) {

  def this() {
    this(null)
  }

  override def writeModelData(): Unit = {
    if(MissingValuesHandling.MeanImputation == model._parms._missing_values_handling) {
      writekv("meanImputation", true)
      writekv("means", model.output.numMeans)
    }

    writekv("weights", model.output.weights)
    writekv("interceptor", model.output.interceptor)
    writekv("defaultThreshold", model._parms._threshold)
    writekv("threshold", model._parms._threshold)
  }

  override def mojoVersion(): String = "1.00"

}
