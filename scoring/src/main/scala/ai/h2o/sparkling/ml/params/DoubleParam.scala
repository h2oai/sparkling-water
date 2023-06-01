package ai.h2o.sparkling.ml.params

import org.json4s.{JDouble, JString, JValue}

object DoubleParam {

  /** Encodes a param value into JValue. */
  def jValueEncode(value: Double): JValue = {
    value match {
      case _ if value.isNaN =>
        JString("NaN")
      case Double.NegativeInfinity =>
        JString("-Inf")
      case Double.PositiveInfinity =>
        JString("Inf")
      case _ =>
        JDouble(value)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode(jValue: JValue): Double = {
    jValue match {
      case JString("NaN") =>
        Double.NaN
      case JString("-Inf") =>
        Double.NegativeInfinity
      case JString("Inf") =>
        Double.PositiveInfinity
      case JDouble(x) =>
        x
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Double.")
    }
  }
}
