package ai.h2o.sparkling.ml.models

import hex.genmodel.easy.RowData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{ExposeUtils, ml, mllib}

/**
  * The object is responsible for conversions between entities representing rows in H2O and Apache Spark.
  */
object RowConverter {

  val temporaryColumnPrefix = "SparklingWater_MOJO_temporary"

  /**
    * Converts a Spark to H2O row data
    */
  def toH2ORowData(row: Row): RowData = new RowData {
    val fieldsWithIndex = row.schema.fields.zipWithIndex
    fieldsWithIndex.foreach {
      case (f, idxRow) =>
        val name =
          if (f.name.startsWith(temporaryColumnPrefix)) f.name.substring(temporaryColumnPrefix.length + 1) else f.name
        if (row.get(idxRow) != null) {
          f.dataType match {
            case BooleanType =>
              put(name, if (row.getBoolean(idxRow)) "True" else "False")
            case BinaryType =>
              row.getAs[Array[Byte]](idxRow).zipWithIndex.foreach {
                case (v, idx) =>
                  put(name + "." + idx, v.toString)
              }
            case ByteType => put(name, row.getByte(idxRow).toString)
            case ShortType => put(name, row.getShort(idxRow).toString)
            case IntegerType => put(name, row.getInt(idxRow).toString)
            case LongType => put(name, row.getLong(idxRow).toString)
            case FloatType => put(name, row.getFloat(idxRow).toString)
            case _: DecimalType => put(name, row.getDecimal(idxRow).doubleValue().toString)
            case DoubleType => put(name, row.getDouble(idxRow).toString)
            case StringType => put(name, row.getString(idxRow))
            case TimestampType => put(name, row.getAs[java.sql.Timestamp](idxRow).getTime.toString)
            case DateType => put(name, row.getAs[java.sql.Date](idxRow).getTime.toString)
            case ArrayType(_, _) => // for now assume that all arrays and vecs have the same size - we can store max size as part of the model
              row.getAs[Seq[_]](idxRow).zipWithIndex.foreach {
                case (v, idx) => put(name + "." + idx, v.toString)
              }
            // WRONG this patter needs to share the same code as in the SparkDataFrameConverter
            // Currently, In SparkDataFrameConverter we handle arrays, binary types and vectors of different size
            // and align them to the same size. The same thing should be done here
            case v if ExposeUtils.isMLVectorUDT(v) =>
              val vector = row.getAs[ml.linalg.Vector](idxRow)
              (0 until vector.size).foreach { idx =>
                put(name + "." + idx, vector(idx).toString)
              }
            case _: mllib.linalg.VectorUDT =>
              val vector = row.getAs[mllib.linalg.Vector](idxRow)
              (0 until vector.size).foreach { idx =>
                put(name + "." + idx, vector(idx).toString)
              }
            case udt if ExposeUtils.isUDT(udt) =>
              throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
            case null => // no op
            case _ => put(name, get(idxRow).toString)
          }
        }
    }
  }
}
