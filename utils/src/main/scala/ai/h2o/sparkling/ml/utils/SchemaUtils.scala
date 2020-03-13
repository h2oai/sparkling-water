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

package ai.h2o.sparkling.ml.utils

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.{ExposeUtils, ml, mllib}

import scala.collection.mutable.ArrayBuffer

/**
  * Utilities for working with Spark SQL component.
  */
object SchemaUtils {

  def flattenDataFrame(df: DataFrame): DataFrame = DatasetShape.getDatasetShape(df.schema) match {
    case DatasetShape.Flat => df
    case DatasetShape.StructsOnly => flattenStructsInDataFrame(df)
    case DatasetShape.Nested =>
      if (df.isStreaming) {
        throw new UnsupportedOperationException(
          "Flattening streamed data frames with an ArrayType, BinaryType or MapType is not supported.")
      } else {
        val schema = flattenSchema(df)
        flattenDataFrame(df, schema)
      }
  }

  def flattenDataFrame(df: DataFrame, flatSchema: StructType): DataFrame = {
    implicit val rowEncoder = RowEncoder(flatSchema)
    val numberOfColumns = flatSchema.fields.length
    val nameToIndexMap = flatSchema.fields.map(_.name).zipWithIndex.toMap
    val originalSchema = df.schema
    df.map[Row] { row: Row =>
      val result = ArrayBuffer.fill[Any](numberOfColumns)(null)
      val fillBufferPartiallyApplied = fillBuffer(nameToIndexMap, result) _
      var idx = 0
      val fields = originalSchema.fields
      while (idx < fields.length) {
        val field = fields(idx)
        fillBufferPartiallyApplied(field.name, field.dataType, row(idx))
        idx = idx + 1
      }
      new GenericRowWithSchema(result.toArray, flatSchema)
    }
  }

  private def fillBuffer
  (flatSchemaIndexes: Map[String, Int], buffer: ArrayBuffer[Any])
  (qualifiedName: String, dataType: DataType, data: Any) = {
    if (data != null) {
      dataType match {
        case BinaryType => fillBinary(qualifiedName, ByteType, flatSchemaIndexes, buffer, data)
        case m: MapType => fillMap(qualifiedName, m.valueType, flatSchemaIndexes, buffer, data)
        case a: ArrayType => fillArray(qualifiedName, a.elementType, flatSchemaIndexes, buffer, data)
        case s: StructType => fillStruct(qualifiedName, s.fields, flatSchemaIndexes, buffer, data)
        case _ => buffer(flatSchemaIndexes(qualifiedName)) = data
      }
    }
  }

  private def fillBinary(
                          qualifiedName: String,
                          elementType: DataType,
                          flatSchemaIndexes: Map[String, Int],
                          buffer: ArrayBuffer[Any],
                          data: Any): Unit = {
    val array = data.asInstanceOf[Array[Byte]]
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    var idx = 0
    while (idx < array.length) {
      val fieldQualifiedName = getQualifiedName(qualifiedName, idx.toString)
      fillBufferPartiallyApplied(fieldQualifiedName, elementType, array(idx))
      idx = idx + 1
    }
  }

  private def fillArray(
                         qualifiedName: String,
                         elementType: DataType,
                         flatSchemaIndexes: Map[String, Int],
                         buffer: ArrayBuffer[Any],
                         data: Any): Unit = {
    val seq = data.asInstanceOf[Seq[Any]]
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    var idx = 0
    while (idx < seq.length) {
      val fieldQualifiedName = getQualifiedName(qualifiedName, idx.toString)
      fillBufferPartiallyApplied(fieldQualifiedName, elementType, seq(idx))
      idx = idx + 1
    }
  }

  private def fillMap(
                       qualifiedName: String,
                       valueType: DataType,
                       flatSchemaIndexes: Map[String, Int],
                       buffer: ArrayBuffer[Any],
                       data: Any): Unit = {
    val map = data.asInstanceOf[Map[Any, Any]]
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    map.foreach { case (key, value) =>
      val fieldQualifiedName = getQualifiedName(qualifiedName, key.toString)
      fillBufferPartiallyApplied(fieldQualifiedName, valueType, value)
    }
  }

  private def fillStruct(
                          qualifiedName: String,
                          fields: Seq[StructField],
                          flatSchemaIndexes: Map[String, Int],
                          buffer: ArrayBuffer[Any],
                          data: Any): Unit = {
    val subRow = data.asInstanceOf[Row]
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    fields.zip(subRow.toSeq).foreach { case (subField, value) =>
      val fieldQualifiedName = getQualifiedName(qualifiedName, subField.name)
      fillBufferPartiallyApplied(fieldQualifiedName, subField.dataType, value)
    }
  }

  def flattenSchema(df: DataFrame): StructType = {
    val rowSchemas = rowsToRowSchemas(df)
    val mergedSchema = mergeRowSchemas(rowSchemas)
    StructType(mergedSchema.map(_.field))
  }

  def rowsToRowSchemas(df: DataFrame): Dataset[ArrayBuffer[FieldWithOrder]] = {
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[ArrayBuffer[FieldWithOrder]]
    val originalSchema = df.schema
    df.map[ArrayBuffer[FieldWithOrder]] { row: Row =>
      var i = 0
      val fields = originalSchema.fields
      val result = new ArrayBuffer[FieldWithOrder]()
      while (i < fields.length) {
        val StructField(name, dataType, nullable, metadata) = fields(i)
        result ++= flattenField(name, dataType, nullable, metadata, row(i), i :: Nil)
        i = i + 1
      }
      result.sortBy(_.order)(fieldPathOrdering)
    }
  }

  private def mergeRowSchemas(ds: Dataset[ArrayBuffer[FieldWithOrder]]): ArrayBuffer[FieldWithOrder] = ds.reduce {
    (first, second) =>
      var idxForFirst = 0
      var idxForSecond = 0
      val result = new ArrayBuffer[FieldWithOrder]()
      while (idxForFirst < first.length && idxForSecond < second.length) {
        val itemFromFirst = first(idxForFirst)
        val itemFromSecond = second(idxForSecond)
        if (fieldPathOrdering.lt(itemFromFirst.order, itemFromSecond.order)) {
          result += itemFromFirst.copy(field = itemFromFirst.field.copy(nullable = true))
          idxForFirst = idxForFirst + 1
        } else if (fieldPathOrdering.gt(itemFromFirst.order, itemFromSecond.order)) {
          result += itemFromSecond.copy(field = itemFromSecond.field.copy(nullable = true))
          idxForSecond = idxForSecond + 1
        } else {
          result += itemFromFirst.copy(
            field = itemFromFirst.field.copy(
              nullable = itemFromFirst.field.nullable || itemFromSecond.field.nullable))
          idxForFirst = idxForFirst + 1
          idxForSecond = idxForSecond + 1
        }
      }
      while (idxForFirst < first.length) {
        val itemFromFirst = first(idxForFirst)
        result += itemFromFirst.copy(field = itemFromFirst.field.copy(nullable = true))
        idxForFirst = idxForFirst + 1

      }
      while (idxForSecond < second.length) {
        val itemFromSecond = second(idxForSecond)
        result += itemFromSecond.copy(field = itemFromSecond.field.copy(nullable = true))
        idxForSecond = idxForSecond + 1
      }
      result
  }

  @transient private lazy val fieldPathOrdering = {
    val segmentOrdering = new Ordering[Any] {
      override def compare(x: Any, y: Any): Int = (x, y) match {
        case (a: Int, b: Int) => a.compareTo(b)
        case (a: String, b: String) => a.compareTo(b)
        case (a, b) => a.toString.compareTo(b.toString)
      }
    }
    Ordering.Iterable(segmentOrdering)
  }

  private def getQualifiedName(prefix: String, name: String): String = prefix + "." + name

  private def flattenField(
                            qualifiedName: String,
                            dataType: DataType,
                            nullable: Boolean,
                            metadata: Metadata,
                            data: Any,
                            path: List[Any]): Seq[FieldWithOrder] = {
    if (data != null) {
      dataType match {
        case BinaryType =>
          flattenBinaryType(qualifiedName, ByteType, nullable, metadata, data, path)
        case MapType(_, valueType, containsNull) =>
          flattenMapType(qualifiedName, valueType, containsNull || nullable, metadata, data, path)
        case ArrayType(elementType, containsNull) =>
          flattenArrayType(qualifiedName, elementType, containsNull || nullable, metadata, data, path)
        case StructType(fields) =>
          flattenStructType(qualifiedName, nullable, metadata, fields, data, path)
        case dt =>
          FieldWithOrder(StructField(qualifiedName, dt, nullable, metadata), path.reverse) :: Nil
      }
    } else {
      Nil
    }
  }

  case class FieldWithOrder(field: StructField, order: Iterable[Any])

  private def flattenBinaryType(
                                 qualifiedName: String,
                                 elementType: DataType,
                                 nullable: Boolean,
                                 metadata: Metadata,
                                 data: Any,
                                 path: List[Any]) = {
    val values = data.asInstanceOf[Array[Byte]]
    val result = new ArrayBuffer[FieldWithOrder]()
    var idx = 0
    while (idx < values.length) {
      val fieldQualifiedName = getQualifiedName(qualifiedName, idx.toString())
      result ++= flattenField(fieldQualifiedName, elementType, nullable, metadata, values(idx), idx :: path)
      idx = idx + 1
    }
    result
  }

  private def flattenArrayType(
                                qualifiedName: String,
                                elementType: DataType,
                                nullable: Boolean,
                                metadata: Metadata,
                                data: Any,
                                path: List[Any]) = {
    val values = data.asInstanceOf[Seq[Any]]
    val result = new ArrayBuffer[FieldWithOrder]()
    var idx = 0
    while (idx < values.length) {
      val fieldQualifiedName = getQualifiedName(qualifiedName, idx.toString())
      result ++= flattenField(fieldQualifiedName, elementType, nullable, metadata, values(idx), idx :: path)
      idx = idx + 1
    }
    result
  }

  private def flattenMapType(
                              qualifiedName: String,
                              valueType: DataType,
                              nullable: Boolean,
                              metadata: Metadata,
                              data: Any,
                              path: List[Any]) = {
    val map = data.asInstanceOf[Map[Any, Any]]
    val result = new ArrayBuffer[FieldWithOrder]()
    map.foreach { case (key, value) =>
      val fieldQualifiedName = getQualifiedName(qualifiedName, key.toString)
      result ++= flattenField(fieldQualifiedName, valueType, nullable, metadata, value, key :: path)
    }
    result
  }

  private def flattenStructType(
                                 qualifiedName: String,
                                 nullableParent: Boolean,
                                 metadata: Metadata,
                                 fields: Seq[StructField],
                                 data: Any,
                                 path: List[Any]) = {
    val subRow = data.asInstanceOf[Row]
    fields.zipWithIndex.flatMap { case (subField, idx) =>
      val StructField(name, dataType, nullable, fieldMetadata) = subField
      val metadataBuilder = new MetadataBuilder()
      metadataBuilder.withMetadata(metadata)
      metadataBuilder.withMetadata(fieldMetadata)
      val mergedMetadata = metadataBuilder.build()
      val fieldQualifiedName = getQualifiedName(qualifiedName, name)
      flattenField(fieldQualifiedName, dataType, nullable || nullableParent, mergedMetadata, subRow(idx), idx :: path)
    }
  }

  def flattenStructsInSchema(
                              schema: StructType,
                              sourceColPrefix: Option[String] = None,
                              targetColPrefix: Option[String] = None,
                              nullable: Boolean = false): Seq[(StructField, String)] = {

    val flattened = schema.fields.flatMap { f =>
      val escaped = if (f.name.contains(".")) "`" + f.name + "`" else f.name
      val colName = if (sourceColPrefix.isDefined) sourceColPrefix.get + "." + escaped else escaped
      val newName = if (targetColPrefix.isDefined) targetColPrefix.get + "." + f.name else f.name

      f.dataType match {
        case st: StructType => flattenStructsInSchema(st, Some(colName), Some(newName), nullable || f.nullable)
        case _ => Array((StructField(newName, f.dataType, nullable || f.nullable), colName))
      }
    }
    flattened
  }

  def flattenStructsInDataFrame(df: DataFrame): DataFrame = {
    val flatten = flattenStructsInSchema(df.schema)
    val cols = flatten.map {
      case (field, colName) => col(colName).as(field.name)
    }
    df.select(cols: _*)
  }

  def appendFlattenedStructsToDataFrame(df: DataFrame, prefixForNewColumns: String): DataFrame = {
    import org.apache.spark.sql.DatasetExtensions._
    val structsOnlySchema = StructType(df.schema.fields.filter(_.dataType.isInstanceOf[StructType]))
    val flatten = flattenStructsInSchema(structsOnlySchema, targetColPrefix = Some(prefixForNewColumns))
    flatten.foldLeft(df) { case (tempDF, (field, colName)) =>
      tempDF.withColumn(field.name, col(colName), field.metadata)
    }
  }

  /** Returns expanded schema
    *  - schema is represented as list of types
    *  - all arrays are expanded into columns based on the longest one
    *  - all vectors are expanded into columns based on the longest one
    *
    * @param flatSchema flat schema of spark data frame
    * @return list of types with their positions
    */
  def expandedSchema(flatSchema: StructType, elemMaxSizes: Array[Int]): Seq[StructField] = {

    val expandedSchema = flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case v if ExposeUtils.isAnyVectorUDT(v) =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, DoubleType, nullable = true)
          }
        case udt if ExposeUtils.isUDT(udt) => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
        case _ => Seq(field)
      }
    }
    expandedSchema
  }

  private def fieldSizeFromMetadata(field: StructField): Option[Int] = {
    field.dataType match {
      case v if ExposeUtils.isMLVectorUDT(v) =>
        Some(AttributeGroup.fromStructField(field).size).filter(_ != -1)
      case _ => None
    }
  }

  /**
    * Collect max size of each element in DataFrame.
    * For array -> max array size
    * For vectors -> max vector size
    * For simple types -> 1
    *
    * @return array containing size of each element
    */
  def collectMaxElementSizes(flatDataFrame: DataFrame): Array[Int] = {
    val vectorIndices = collectVectorLikeTypes(flatDataFrame.schema)
    val simpleIndices = collectSimpleLikeTypes(flatDataFrame.schema)

    val sizeFromMetadata = vectorIndices.map(idx => fieldSizeFromMetadata(flatDataFrame.schema.fields(idx)))
    val maxCollectionSizes = if (sizeFromMetadata.forall(_.isDefined)) {
      sizeFromMetadata.map(_.get).toArray
    } else {
      val sizes = flatDataFrame.rdd.map { row =>
        vectorIndices.map { idx => getCollectionSize(row, idx) }
      }
      if (sizes.isEmpty) {
        Array(0)
      } else {
        sizes.reduce((a, b) => a.indices.map(i => if (a(i) > b(i)) a(i) else b(i))).toArray
      }
    }

    val collectionIdxToSize = vectorIndices.zip(maxCollectionSizes).toMap
    val simpleTypeIdxToSize = simpleIndices.zip(Array.fill(simpleIndices.length)(1)).toMap
    val elemTypeIdxToSize = collectionIdxToSize ++ simpleTypeIdxToSize
    val elemSizeArray = elemTypeIdxToSize.toSeq.sortBy(_._1).map(_._2)

    elemSizeArray.toArray
  }

  def collectVectorLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case v if ExposeUtils.isMLVectorUDT(v) => Some(idx)
        case _: mllib.linalg.VectorUDT => Some(idx)
        case _ => None
      }
    }
  }

  private def collectSimpleLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case BooleanType => Some(idx)
        case ByteType => Some(idx)
        case ShortType => Some(idx)
        case IntegerType => Some(idx)
        case LongType => Some(idx)
        case FloatType => Some(idx)
        case _: DecimalType => Some(idx)
        case DoubleType => Some(idx)
        case StringType => Some(idx)
        case TimestampType => Some(idx)
        case DateType => Some(idx)
        case _ => None
      }
    }
  }

  /**
    * Get size of Array or Vector type. Expects already flattened DataFrame
    *
    * @param row current row
    * @param idx index of the element
    */
  private def getCollectionSize(row: Row, idx: Int): Int = {
    if (row.schema == null || row.isNullAt(idx)) {
      0
    } else {
      val dataType = row.schema.fields(idx).dataType
      dataType match {
        case v if ExposeUtils.isMLVectorUDT(v) => row.getAs[ml.linalg.Vector](idx).size
        case _: mllib.linalg.VectorUDT => row.getAs[mllib.linalg.Vector](idx).size
        case udt if ExposeUtils.isUDT(udt) => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
      }
    }
  }
}
