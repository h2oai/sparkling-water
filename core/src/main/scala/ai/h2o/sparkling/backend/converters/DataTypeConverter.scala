package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.utils.SupportedTypes
import ai.h2o.sparkling.extensions.serde.ExpectedTypes
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import org.apache.spark.ExposeUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import water.fvec.Vec
import water.parser.{BufferedString, Categorical, PreviewParseWriter}

private[backend] object DataTypeConverter {

  def determineExpectedTypes(schema: StructType): Array[ExpectedType] = {
    schema.map {
      case field =>
        field.dataType match {
          case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] =>
            ExpectedTypes.Double
          case BooleanType => ExpectedTypes.Categorical
          case StringType => ExpectedTypes.Categorical
          case v if ExposeUtils.isAnyVectorUDT(v) => ExpectedTypes.Vector
          case dt: DataType => SupportedTypes.bySparkType(dt).expectedType
        }
    }.toArray
  }

  def expectedTypesFromClasses(classes: Array[Class[_]]): Array[ExpectedType] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ExpectedTypes.Bool
      } else if (clazz == classOf[java.lang.Byte]) {
        ExpectedTypes.Byte
      } else if (clazz == classOf[java.lang.Short]) {
        ExpectedTypes.Short
      } else if (clazz == classOf[java.lang.Character]) {
        ExpectedTypes.Char
      } else if (clazz == classOf[java.lang.Integer]) {
        ExpectedTypes.Int
      } else if (clazz == classOf[java.lang.Long]) {
        ExpectedTypes.Long
      } else if (clazz == classOf[java.lang.Float]) {
        ExpectedTypes.Float
      } else if (clazz == classOf[java.lang.Double]) {
        ExpectedTypes.Double
      } else if (clazz == classOf[java.lang.String]) {
        ExpectedTypes.String
      } else if (clazz == classOf[java.sql.Timestamp] || clazz == classOf[java.sql.Date]) {
        ExpectedTypes.Timestamp
      } else if (clazz == classOf[org.apache.spark.ml.linalg.Vector]) {
        ExpectedTypes.Vector
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }
}
