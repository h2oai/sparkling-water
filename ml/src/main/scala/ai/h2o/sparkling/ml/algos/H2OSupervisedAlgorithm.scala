package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OSupervisedMOJOModel

import ai.h2o.sparkling.{H2OColumnType, H2OFrame}
import hex.Model
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

abstract class H2OSupervisedAlgorithm[P <: Model.Parameters: ClassTag] extends H2OAlgorithm[P] {

  def getLabelCol(): String

  def getOffsetCol(): String

  def getWeightCol(): String

  def setLabelCol(value: String): this.type

  def setOffsetCol(value: String): this.type

  def setWeightCol(value: String): this.type

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    require(
      schema.fields.exists(f => f.name.compareToIgnoreCase(getLabelCol()) == 0),
      s"Specified label column '${getLabelCol()} was not found in input dataset!")
    require(
      !getFeaturesCols().exists(n => n.compareToIgnoreCase(getLabelCol()) == 0),
      "Specified input features cannot contain the label column!")
    transformedSchema
  }

  override def fit(dataset: Dataset[_]): H2OSupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OSupervisedMOJOModel]
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getLabelCol(), getWeightCol(), getOffsetCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
