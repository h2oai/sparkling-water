package ai.h2o.sparkling.ml.params

import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.{BooleanParam, Params, StringArrayParam}
import org.apache.spark.sql.types.StructType

trait H2OMOJOPipelineParams extends Params with Logging {

  protected final val removeModel: BooleanParam =
    new BooleanParam(this, "removeModel", "The modelling part is removed from MOJO pipeline.")

  // private parameter used to store MOJO output columns
  protected final val outputCols: StringArrayParam =
    new StringArrayParam(this, "outputCols", "Name of output columns produced by the MOJO.")

  protected final val mojoInputSchema: StructTypeParam = new StructTypeParam(
    this,
    "mojoInputSchema",
    "Expected input schema by the MOJO model (for example, if you need to define data parser).")

  protected final val mojoOutputSchema: StructTypeParam = new StructTypeParam(
    this,
    "mojoOutputSchema",
    "Exposed output schema by the MOJO model (this is not prediction schema).")

  protected final val expandNamedMojoOutputColumns: BooleanParam =
    new BooleanParam(this, "expandNamedMojoOutputColumns", "Expand output named columns.")

  setDefault(removeModel -> false, expandNamedMojoOutputColumns -> false)

  //
  // Getters
  //
  def getRemoveModel(): Boolean = $(removeModel)

  def getOutputCols(): Array[String] = $(outputCols)

  def getMojoInputSchema(): StructType = $(mojoInputSchema)

  def getMojoOutputSchema(): StructType = $(mojoOutputSchema)

  def getExpandNamedMojoOutputColumns(): Boolean = $(expandNamedMojoOutputColumns)
}
