package ai.h2o.sparkling.doc.generation

object ParametersTocTreeTemplate {
  def apply(algorithms: Seq[Class[_]], featureTransformers: Seq[Class[_]]): String = {
    val algorithmItems = algorithms.map(algorithm => s"   parameters_${algorithm.getSimpleName}").mkString("\n")
    val featureItems = featureTransformers.map(feature => s"   parameters_${feature.getSimpleName}").mkString("\n")
    s"""Algorithm Parameters
       |====================
       |
       |**Algorithms**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$algorithmItems
       |
       |**Feature Transformers**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$featureItems
    """.stripMargin
  }
}
