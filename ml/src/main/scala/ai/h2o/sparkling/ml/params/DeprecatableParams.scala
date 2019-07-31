package ai.h2o.sparkling.ml.params

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{Param, Params}

/**
  * The trait represents a definition of ML algorithm parameters that may contain some deprecations
  */
trait DeprecatableParams extends Params with Logging {

  /**
    * When a parameter is renamed, the mapping 'old name' -> 'new name' should be added into this map.
    */
  protected def renamingMap: Map[String, String]

  private def applyRenaming(parameterName: String): String = renamingMap.getOrElse(parameterName, parameterName)

  override def hasParam(paramName: String): Boolean = super.hasParam(applyRenaming(paramName))

  override def getParam(paramName: String): Param[Any] = super.getParam(applyRenaming(paramName))
}
