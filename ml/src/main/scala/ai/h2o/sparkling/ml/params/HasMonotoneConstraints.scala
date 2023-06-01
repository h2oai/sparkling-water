package ai.h2o.sparkling.ml.params

import java.util

import ai.h2o.sparkling.H2OFrame

import scala.collection.JavaConverters._

trait HasMonotoneConstraints extends H2OAlgoParamsBase {
  private val monotoneConstraints = new DictionaryParam(
    this,
    "monotoneConstraints",
    "A key must correspond to a feature name and value could be 1 or -1")

  setDefault(monotoneConstraints -> new util.HashMap[String, Double]())

  def getMonotoneConstraints(): Map[String, Double] = $(monotoneConstraints).asScala.toMap

  def setMonotoneConstraints(value: Map[String, Double]): this.type = set(monotoneConstraints, value.asJava)

  private[sparkling] def getMonotoneConstraintsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("monotone_constraints" -> getMonotoneConstraints())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("monotoneConstraints" -> "monotone_constraints")
  }
}
