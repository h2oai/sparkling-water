package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame

trait HasUnsupportedOffsetCol {

  def getOffsetCol(): String = null

  def setOffsetCol(value: String): this.type = {
    val className = this.getClass.getName
    throw new UnsupportedOperationException(s"The parameter 'offsetCol' is not yet supported on $className.")
  }

  private[sparkling] def getUnsupportedOffsetColParam(trainingFrame: H2OFrame): Map[String, Any] = Map.empty
}
