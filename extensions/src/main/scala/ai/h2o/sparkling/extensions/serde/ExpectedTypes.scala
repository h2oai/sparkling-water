package ai.h2o.sparkling.extensions.serde

object ExpectedTypes extends Enumeration {
  type ExpectedType = Value
  val Bool, Byte, Char, Short, Int, Float, Long, Double, String, Timestamp, Vector, Categorical = Value
}
