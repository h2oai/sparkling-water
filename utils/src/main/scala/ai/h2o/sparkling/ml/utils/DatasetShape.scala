package ai.h2o.sparkling.ml.utils

import org.apache.spark.sql.types._

object DatasetShape extends Enumeration {
  type DatasetShape = Value
  val Flat, StructsOnly, Nested = Value

  def getDatasetShape(schema: StructType): DatasetShape = {
    def mergeShape(first: DatasetShape, second: DatasetShape): DatasetShape = (first, second) match {
      case (DatasetShape.Nested, _) => DatasetShape.Nested
      case (_, DatasetShape.Nested) => DatasetShape.Nested
      case (DatasetShape.StructsOnly, _) => DatasetShape.StructsOnly
      case (_, DatasetShape.StructsOnly) => DatasetShape.StructsOnly
      case _ => DatasetShape.Flat
    }

    def fieldToShape(field: StructField): DatasetShape = field.dataType match {
      case _: ArrayType | _: MapType | _: BinaryType => DatasetShape.Nested
      case s: StructType => mergeShape(DatasetShape.StructsOnly, getDatasetShape(s))
      case _ => DatasetShape.Flat
    }

    schema.fields.foldLeft(DatasetShape.Flat) { (acc, field) =>
      val fieldShape = fieldToShape(field)
      mergeShape(acc, fieldShape)
    }
  }
}
