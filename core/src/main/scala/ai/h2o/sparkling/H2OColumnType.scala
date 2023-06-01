package ai.h2o.sparkling

object H2OColumnType extends Enumeration {
  val enum, string, int, real, time, uuid = Value

  def fromString(dataType: String): Value = {
    values.find(_.toString == dataType).getOrElse(throw new RuntimeException(s"Unknown H2O's Data type $dataType"))
  }
}
