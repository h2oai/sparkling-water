package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.models.H2OMOJOSettings
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.{BooleanParam, Param, Params, StringArrayParam}

trait H2OAlgorithmMOJOParams extends H2OBaseMOJOParams with Logging {

  //
  // Param definitions
  //
  protected final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")

  protected final val detailedPredictionCol = new Param[String](
    this,
    "detailedPredictionCol",
    "Column containing additional prediction details, its content depends on the model type.")

  protected final val withDetailedPredictionCol = new BooleanParam(
    this,
    "withDetailedPredictionCol",
    "Enables or disables generating additional prediction column, but with more details")

  protected final val withContributions = new BooleanParam(
    this,
    "withContributions",
    "Enables or disables generating a sub-column of detailedPredictionCol containing Shapley values.")

  protected final val featuresCols: StringArrayParam =
    new StringArrayParam(this, "featuresCols", "Name of feature columns")

  protected final val namedMojoOutputColumns: Param[Boolean] = new BooleanParam(
    this,
    "namedMojoOutputColumns",
    "Mojo Output is not stored in the array but in the properly named columns")

  protected final val withLeafNodeAssignments =
    new BooleanParam(this, "withLeafNodeAssignments", "Enables or disables computation of leaf node assignments.")

  protected final val withStageResults =
    new BooleanParam(this, "withStageResults", "Enables or disables computation of stage results.")

  //
  // Default values
  //
  setDefault(
    predictionCol -> H2OMOJOSettings.default.predictionCol,
    detailedPredictionCol -> H2OMOJOSettings.default.detailedPredictionCol,
    withContributions -> H2OMOJOSettings.default.withContributions,
    featuresCols -> Array.empty[String],
    namedMojoOutputColumns -> H2OMOJOSettings.default.namedMojoOutputColumns,
    withLeafNodeAssignments -> H2OMOJOSettings.default.withLeafNodeAssignments,
    withStageResults -> H2OMOJOSettings.default.withStageResults)

  //
  // Getters
  //
  def getPredictionCol(): String = $(predictionCol)

  def getDetailedPredictionCol(): String = $(detailedPredictionCol)

  @DeprecatedMethod(version = "3.36")
  def getWithDetailedPredictionCol(): Boolean = true

  def getWithContributions(): Boolean = $(withContributions)

  def getFeaturesCols(): Array[String] = $(featuresCols)

  def getNamedMojoOutputColumns(): Boolean = $(namedMojoOutputColumns)

  def getWithLeafNodeAssignments(): Boolean = $(withLeafNodeAssignments)

  def getWithStageResults(): Boolean = $(withStageResults)
}
