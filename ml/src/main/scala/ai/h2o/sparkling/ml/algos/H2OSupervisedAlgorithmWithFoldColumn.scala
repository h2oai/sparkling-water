package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OSupervisedMOJOModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import hex.Model

import scala.reflect.ClassTag

abstract class H2OSupervisedAlgorithmWithFoldColumn[P <: Model.Parameters: ClassTag] extends H2OSupervisedAlgorithm[P] {

  def getFoldCol(): String

  def setFoldCol(value: String): this.type

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    require(
      getOffsetCol() == null || getOffsetCol() != getFoldCol(),
      "Specified offset column cannot be the same as the fold column!")
    require(
      getWeightCol() == null || getWeightCol() != getFoldCol(),
      "Specified weight column cannot be the same as the fold column!")
    transformedSchema
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
