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

package org.apache.spark.h2o.utils

import org.apache.spark.h2o._
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{ml, mllib}

import scala.collection.mutable.ArrayBuffer

/**
  * Utilities for working with Spark SQL component.
  */
object H2OSchemaUtils {

  import ReflectionUtils._

  def createSchema[T <: Frame](f: T, copyMetadata: Boolean): StructType = {
    val types = new Array[StructField](f.numCols())
    val vecs = f.vecs()
    val names = f.names()
    for (i <- 0 until f.numCols()) {
      val vec = vecs(i)
      types(i) = if (copyMetadata) {
        var metadata = (new MetadataBuilder).
          putLong("count", vec.length()).
          putLong("naCnt", vec.naCnt())

        if (vec.isCategorical) {
          metadata = metadata.putStringArray("vals", vec.domain()).
            putLong("cardinality", vec.cardinality().toLong)
        } else if (vec.isNumeric) {
          metadata = metadata.
            putDouble("min", vec.min()).
            putDouble("mean", vec.mean()).
            putDoubleArray("percentiles", vec.pctiles()).
            putDouble("max", vec.max()).
            putDouble("std", vec.sigma()).
            putDouble("sparsity", vec.nzCnt() / vec.length().toDouble)
        }
        StructField(
          names(i), // Name of column
          dataTypeFor(vec), // Catalyst type of column
          vec.naCnt() > 0,
          metadata.build())
      } else {
        StructField(
          names(i), // Name of column
          dataTypeFor(vec), // Catalyst type of column
          vec.naCnt() > 0)
      }
    }
    StructType(types)
  }

  def isSchemaFlat(schema: StructType): Boolean = schema.fields.forall {
    field: StructField => field.dataType match {
      case _: BinaryType | _: ArrayType | _: StructType | _: MapType => false
      case _ => true
    }
  }

  def flattenDataFrame(df: DataFrame): DataFrame = {
    if (isSchemaFlat(df.schema)) {
      df
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
        case BinaryType =>
          val binaryData = data.asInstanceOf[Array[Byte]].toSeq
          fillArray(qualifiedName, ByteType, flatSchemaIndexes, buffer, binaryData)
        case m: MapType => fillMap(qualifiedName, m.valueType, flatSchemaIndexes, buffer, data)
        case a: ArrayType => fillArray(qualifiedName, a.elementType, flatSchemaIndexes, buffer, data)
        case s: StructType => fillStruct(qualifiedName, s.fields, flatSchemaIndexes, buffer, data)
        case _ => buffer(flatSchemaIndexes(qualifiedName)) = data
      }
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
      var fidx = 0
      var sidx = 0
      val result = new ArrayBuffer[FieldWithOrder]()
      while (fidx < first.length && sidx < second.length) {
        val f = first(fidx)
        val s = second(sidx)
        if (fieldPathOrdering.lt(f.order, s.order)) {
          result += f.copy(field = f.field.copy(nullable = true))
          fidx = fidx + 1
        } else if (fieldPathOrdering.gt(f.order, s.order)) {
          result += s.copy(field = s.field.copy(nullable = true))
          sidx = sidx + 1
        } else {
          result += f.copy(field = f.field.copy(nullable = f.field.nullable || s.field.nullable))
          fidx = fidx + 1
          sidx = sidx + 1
        }
      }
      while (fidx < first.length) {
        val f = first(fidx)
        result += f.copy(field = f.field.copy(nullable = true))
        fidx = fidx + 1

      }
      while (sidx < second.length) {
        val s = second(sidx)
        result += s.copy(field = s.field.copy(nullable = true))
        sidx = sidx + 1
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

  private def getQualifiedName(prefix: String, name: String): String = prefix + "_" + name

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
          val binaryData = data.asInstanceOf[Array[Byte]].toSeq
          flattenArrayType(qualifiedName, ByteType, nullable, metadata, binaryData, path)
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
    val subRow = Row.fromSeq(map.values.toSeq)
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
      val mergedMetedata = metadataBuilder.build()
      val fieldQualifiedName = getQualifiedName(qualifiedName, name)
      flattenField(fieldQualifiedName, dataType, nullable || nullableParent, mergedMetedata, subRow(idx), idx :: path)
    }
  }

  def flattenStructsInSchema(schema: StructType, prefix: String = null, nullable: Boolean = false): StructType = {

    val flattened = schema.fields.flatMap { f =>
      val escaped = if (f.name.contains(".")) "`" + f.name + "`" else f.name
      val colName = if (prefix == null) escaped else prefix + "." + escaped

      f.dataType match {
        case st: StructType => flattenStructsInSchema(st, colName, nullable || f.nullable)
        case _ => Array[StructField](StructField(colName, f.dataType, nullable || f.nullable))
      }
    }
    StructType(flattened)
  }

  def flattenStructsInDataFrame(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.col
    val flatten = flattenStructsInSchema(df.schema)
    val cols = flatten.map(f => col(f.name).as(f.name.replaceAll("`", "")))
    df.select(cols: _*)
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
        case _ : ml.linalg.VectorUDT | _: mllib.linalg.VectorUDT  =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, DoubleType, nullable = true)
          }
        case udt: UserDefinedType[_] => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
        case _ => Seq(field)
      }
    }
    expandedSchema
  }

  /** Returns array for all expanded elements with values true/false based on the fact whether the value is sparse
    * vector or not
    *  - schema is represented as list of types
    *  - all arrays are expanded into columns based on the longest one
    *  - all vectors are expanded into columns based on the longest one
    *
    * @param flatDataFrame flat data framez
    * @param elemMaxSizes  max sizes of each element in the dataframe
    * @return list of types with their positions
    */
  def collectSparseInfo(flatDataFrame: DataFrame, elemMaxSizes: Array[Int]): Array[Boolean] = {
    val vectorIndices = collectVectorLikeTypes(flatDataFrame.schema)
    val sparseInfoForVec = flatDataFrame.take(1).headOption.map { head =>
      vectorIndices.map { idx =>
        head.get(idx) match {
          case _: ml.linalg.SparseVector | _: mllib.linalg.SparseVector => (idx, true)
          case _ => (idx, false)
        }
      }.toMap
    }.getOrElse {
      vectorIndices.zip(Array.fill(vectorIndices.length)(false)).toMap
    }

    flatDataFrame.schema.fields.zipWithIndex.flatMap { case (field, idx) =>

      field.dataType match {
        case _: ml.linalg.VectorUDT | _: mllib.linalg.VectorUDT =>
          Array.fill(elemMaxSizes(idx))(sparseInfoForVec(idx)).toSeq
        case _ => Seq(false)
      }
    }
  }

  /**
    * Mapping from row in Spark frame to position where to start filling in H2OFrame
    */

  def collectElemStartPositions(maxElemSizes: Array[Int]): Array[Int] = {
    // empty array filled with zeros
    val startPositions = Array.ofDim[Int](maxElemSizes.length)
    if (maxElemSizes.length == 0) {
      startPositions
    } else {
      startPositions(0) = 0
      (1 until maxElemSizes.length).foreach { idx =>
        startPositions(idx) = startPositions(idx - 1) + maxElemSizes(idx - 1)
      }
      startPositions
    }
  }

  private def fieldSizeFromMetadata(field: StructField): Option[Int] = {
      field.dataType match {
        case _: ml.linalg.VectorUDT =>
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


  def collectStringIndices(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case StringType => Some(idx)
        case _ => None
      }
    }
  }

  def collectArrayLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case ArrayType(_, _) => Some(idx)
        case BinaryType => Some(idx)
        case _ => None
      }
    }
  }

  def collectVectorLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case _: ml.linalg.VectorUDT => Some(idx)
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
        case _: ml.linalg.VectorUDT => row.getAs[ml.linalg.Vector](idx).size
        case _: mllib.linalg.VectorUDT => row.getAs[mllib.linalg.Vector](idx).size
        case udt: UserDefinedType[_] => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
      }
    }
  }

}
