package ai.h2o.sparkling.ml.params

/**
  * Parameters available on the supervised algorithm & supervised MOJO Model
  */
trait H2OSupervisedMOJOParams extends H2OBaseMOJOParams {
  private[sparkling] final val offsetCol = new NullableStringParam(this, "offsetCol", "Offset column name")

  setDefault(offsetCol -> null)

  def getOffsetCol(): String = $(offsetCol)
}
