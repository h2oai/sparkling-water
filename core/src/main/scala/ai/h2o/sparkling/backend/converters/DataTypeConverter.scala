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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.utils.SupportedTypes
import ai.h2o.sparkling.extensions.serde.ChunkSerdeConstants
import org.apache.spark.ExposeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import water.fvec.Vec
import water.parser.{BufferedString, PreviewParseWriter}

private[backend] object DataTypeConverter {

  private def stringTypesToExpectedTypes(rdd: RDD[Row], schema: StructType): Map[Int, Byte] = {
    val stringTypeIndices = for {
      (field, index) <- schema.fields.zipWithIndex
      if field.dataType == StringType
    } yield index

    val types = if (rdd.getNumPartitions > 0) {
      val preview = rdd
        .mapPartitions[CategoricalPreviewWriter](createPartitionPreview(_, stringTypeIndices))
        .reduce((a, b) => CategoricalPreviewWriter.unifyColumnPreviews(a, b))

      preview.guessTypes().map {
        case Vec.T_CAT => ChunkSerdeConstants.EXPECTED_CATEGORICAL
        case _ => ChunkSerdeConstants.EXPECTED_STRING
      }
    } else {
      stringTypeIndices.map(_ => Vec.T_STR)
    }

    stringTypeIndices.zip(types).toMap
  }

  private def createPartitionPreview(rows: Iterator[Row],
                                     stringTypeIndices: Array[Int]): Iterator[CategoricalPreviewWriter] = {
    val previewParseWriter = new CategoricalPreviewWriter(stringTypeIndices.length)
    val bufferedString = new BufferedString()
    var rowId = 0
    while (rows.hasNext && rowId < CategoricalPreviewWriter.MAX_PREVIEW_RECORDS) {
      val row = rows.next()
      var i = 0
      while (i < stringTypeIndices.length) {
        val colId = stringTypeIndices(i)
        val string = row.getString(colId)
        if (string == null) {
          previewParseWriter.addInvalidCol(i)
        } else {
          bufferedString.set(string)
          previewParseWriter.addStrCol(i, bufferedString)
        }
        i += 1
      }
      rowId += 1
    }
    Iterator.single(previewParseWriter)
  }

  def determineExpectedTypes(rdd: RDD[Row], schema: StructType): Array[Byte] = {
    val stringTypes = stringTypesToExpectedTypes(rdd, schema)
    schema.zipWithIndex.map { case (field, index) =>
      field.dataType match {
        case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] =>
          ChunkSerdeConstants.EXPECTED_DOUBLE
        case v if ExposeUtils.isAnyVectorUDT(v) => ChunkSerdeConstants.EXPECTED_VECTOR
        case StringType => stringTypes(index)
        case dt: DataType => SupportedTypes.bySparkType(dt).expectedType
      }
    }.toArray
  }

  def expectedTypesFromClasses(classes: Array[Class[_]]): Array[Byte] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ChunkSerdeConstants.EXPECTED_BOOL
      } else if (clazz == classOf[java.lang.Byte]) {
        ChunkSerdeConstants.EXPECTED_BYTE
      } else if (clazz == classOf[java.lang.Short]) {
        ChunkSerdeConstants.EXPECTED_SHORT
      } else if (clazz == classOf[java.lang.Character]) {
        ChunkSerdeConstants.EXPECTED_CHAR
      } else if (clazz == classOf[java.lang.Integer]) {
        ChunkSerdeConstants.EXPECTED_INT
      } else if (clazz == classOf[java.lang.Long]) {
        ChunkSerdeConstants.EXPECTED_LONG
      } else if (clazz == classOf[java.lang.Float]) {
        ChunkSerdeConstants.EXPECTED_FLOAT
      } else if (clazz == classOf[java.lang.Double]) {
        ChunkSerdeConstants.EXPECTED_DOUBLE
      } else if (clazz == classOf[java.lang.String]) {
        ChunkSerdeConstants.EXPECTED_STRING
      } else if (clazz == classOf[java.sql.Timestamp] || clazz == classOf[java.sql.Date]) {
        ChunkSerdeConstants.EXPECTED_TIMESTAMP
      } else if (clazz == classOf[org.apache.spark.ml.linalg.Vector]) {
        ChunkSerdeConstants.EXPECTED_VECTOR
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }
}
