package ai.h2o.sparkling.doc.generation

object ModelDetailsTocTreeTemplate {
  def apply(algorithmModels: Seq[Class[_]], featureTransformerModels: Seq[Class[_]]): String = {
    val algorithmItems = algorithmModels.map(algorithm => s"   model_details_${algorithm.getSimpleName}").mkString("\n")
    val featureItems =
      featureTransformerModels.map(feature => s"   model_details_${feature.getSimpleName}").mkString("\n")
    s""".. _model_details:
       |
       |Model Details
       |=============
       |
       |**Algorithm Models**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$algorithmItems
       |
       |**Feature Transformer Models**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$featureItems
    """.stripMargin
  }
}
