package ai.h2o.sparkling

case class H2OColumn(
    name: String,
    dataType: H2OColumnType.Value,
    min: Double,
    max: Double,
    mean: Double,
    sigma: Double,
    numberOfZeros: Long,
    numberOfMissingElements: Long,
    domain: Array[String],
    domainCardinality: Long,
    private val percentilesGetter: String => Array[Double]) {
  def nullable: Boolean = numberOfMissingElements > 0

  def isString(): Boolean = dataType == H2OColumnType.string

  def isNumeric(): Boolean = dataType == H2OColumnType.real || dataType == H2OColumnType.int

  def isTime(): Boolean = dataType == H2OColumnType.time

  def isCategorical(): Boolean = dataType == H2OColumnType.`enum`

  def isUUID(): Boolean = dataType == H2OColumnType.uuid

  def percentiles(): Array[Double] = percentilesGetter(name)
}
