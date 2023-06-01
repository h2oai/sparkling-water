package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{BooleanParam, IntParam, Param}
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OGLRMExtraParams extends H2ODimReductionExtraParams {

  private val reconstructedCol: Param[String] = new Param[String](
    parent = this,
    name = "reconstructedCol",
    doc = "Reconstructed column name. This column contains reconstructed input values (A_hat=X*Y instead of just X).")

  private val withReconstructedCol: Param[Boolean] = new BooleanParam(
    parent = this,
    name = "withReconstructedCol",
    doc = "A flag identifying whether a column with reconstructed input values will be produced or not.")

  private val maxScoringIterations: Param[Int] = new IntParam(
    parent = this,
    name = "maxScoringIterations",
    doc = "The maximum number of iterations  used in MOJO scoring to update X")

  setDefault(reconstructedCol -> (uid + "__reconstructed"), withReconstructedCol -> false, maxScoringIterations -> 100)

  //
  // Getters
  //
  def getReconstructedCol(): String = $(reconstructedCol)

  def getWithReconstructedCol(): Boolean = $(withReconstructedCol)

  def getMaxScoringIterations(): Int = $(maxScoringIterations)

  //
  // Setters
  //
  def setReconstructedCol(name: String): this.type = set(reconstructedCol -> name)

  def setWithReconstructedCol(flag: Boolean): this.type = set(withReconstructedCol -> flag)

  def setMaxScoringIterations(value: Int): this.type = set(maxScoringIterations -> value)

  protected override def outputSchema: Seq[StructField] = {
    val outputType = org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    val baseSchema = super.outputSchema

    val withReconstructedFieldSchema = if (getWithReconstructedCol()) {
      val reconstructedField = StructField(getReconstructedCol(), outputType, nullable = false)
      baseSchema :+ reconstructedField
    } else {
      baseSchema
    }

    withReconstructedFieldSchema
  }

  protected override def validate(schema: StructType): Unit = {
    super.validate(schema)
    val fieldNames = schema.fieldNames

    require(getReconstructedCol() != null || !getWithReconstructedCol(), "The reconstructed column can't be null!")
    require(
      !fieldNames.contains(getReconstructedCol()) || !getWithReconstructedCol(),
      s"The reconstructed column '${getReconstructedCol()}' is already present in the dataset!")
  }

  protected override def copyExtraParams(to: H2ODimReductionExtraParams): Unit = {
    super.copyExtraParams(to)
    val toGLRM = to.asInstanceOf[H2OGLRMExtraParams]
    toGLRM.setReconstructedCol(getReconstructedCol())
    toGLRM.setWithReconstructedCol(getWithReconstructedCol())
    toGLRM.setMaxScoringIterations(getMaxScoringIterations())
  }
}
