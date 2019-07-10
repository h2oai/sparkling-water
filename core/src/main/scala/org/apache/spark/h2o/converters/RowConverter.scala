/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.h2o.converters

import hex.genmodel.easy.RowData
import org.apache.spark.{ml, mllib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * The object is responsible for conversions between entities representing rows in H2O and Apache Spark.
  */
object RowConverter {

  /**
    * Converts a Spark to H2O row data
    */
  def toH2ORowData(row: Row): RowData = new RowData {
    val fieldsWithIndex = row.schema.fields.zipWithIndex
    fieldsWithIndex.foreach { case (f, idxRow) =>
      if (row.get(idxRow) != null) {
        f.dataType match {
          case BooleanType =>
            put(f.name, row.getBoolean(idxRow).toString)
          case BinaryType =>
            row.getAs[Array[Byte]](idxRow).zipWithIndex.foreach { case (v, idx) =>
              put(f.name + idx, v.toString)
            }
          case ByteType => put(f.name, row.getByte(idxRow).toString)
          case ShortType => put(f.name, row.getShort(idxRow).toString)
          case IntegerType => put(f.name, row.getInt(idxRow).toString)
          case LongType => put(f.name, row.getLong(idxRow).toString)
          case FloatType => put(f.name, row.getFloat(idxRow).toString)
          case _: DecimalType => put(f.name, row.getDecimal(idxRow).doubleValue().toString)
          case DoubleType => put(f.name, row.getDouble(idxRow).toString)
          case StringType => put(f.name, row.getString(idxRow))
          case TimestampType => put(f.name, row.getAs[java.sql.Timestamp](idxRow).getTime.toString)
          case DateType => put(f.name, row.getAs[java.sql.Date](idxRow).getTime.toString)
          case ArrayType(_, _) => // for now assume that all arrays and vecs have the same size - we can store max size as part of the model
            row.getAs[Seq[_]](idxRow).zipWithIndex.foreach { case (v, idx) =>
              put(f.name + idx, v.toString)
            }
          // WRONG this patter needs to share the same code as in the SparkDataFrameConverter
          // Currently, In SparkDataFrameConverter we handle arrays, binary types and vectors of different size
          // and align them to the same size. The same thing should be done here
          case _: ml.linalg.VectorUDT =>
            val vector = row.getAs[ml.linalg.Vector](idxRow)
            (0 until vector.size).foreach { idx =>
              put(f.name + idx, vector(idx).toString)
            }
          case _: mllib.linalg.VectorUDT =>
            val vector = row.getAs[mllib.linalg.Vector](idxRow)
            (0 until vector.size).foreach { idx =>
              put(f.name + idx, vector(idx).toString)
            }
          case udt: UserDefinedType[_] => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
          case null => // no op
          case _ => put(f.name, get(idxRow).toString)
        }
      }
    }
  }
}
