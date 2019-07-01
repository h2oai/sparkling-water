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

  def flattenDataFrame(df: DataFrame): DataFrame = {
    val schema = flattenSchema(df)
    flattenDataFrame(df, schema)
  }

  def flattenDataFrame(df: DataFrame, flatSchema: StructType): DataFrame = {
    implicit val rowEncoder = RowEncoder(flatSchema)
    val numberOfColumns = flatSchema.fields.length
    val nameToIndexMap = flatSchema.fields.map(_.name).zipWithIndex.toMap
    val originalSchema = df.schema
    df.map[Row] { row: Row =>
      val result = ArrayBuffer.fill[Any](numberOfColumns)(null)
      val fillBufferPartiallyApplied = fillBuffer(nameToIndexMap, result) _
      originalSchema.fields.zipWithIndex.foreach { case (field, idx) =>
        fillBufferPartiallyApplied(field, row(idx), None)
      }
      Row.fromSeq(result)
    }
  }

  private def fillBuffer
  (flatSchemaIndexes: Map[String, Int], buffer: ArrayBuffer[Any])
  (field: StructField, data: Any, prefix: Option[String] = None): Unit = {
    if (data != null) {
      val StructField(name, dataType, _, _) = field
      val qualifiedName = getQualifiedName(prefix, name)
      dataType match {
        case BinaryType => fillArray(ByteType, flatSchemaIndexes, buffer, data, qualifiedName)
        case MapType(_, valueType, _) => fillMap(valueType, flatSchemaIndexes, buffer, data, qualifiedName)
        case ArrayType(elementType, _) => fillArray(elementType, flatSchemaIndexes, buffer, data, qualifiedName)
        case StructType(fields) => fillStruct(fields, flatSchemaIndexes, buffer, data, qualifiedName)
        case _ => buffer(flatSchemaIndexes(qualifiedName)) = data
      }
    }
  }

  private def fillArray(
      elementType: DataType,
      flatSchemaIndexes: Map[String, Int],
      buffer: ArrayBuffer[Any],
      data: Any,
      qualifiedName: String): Unit = {
    val seq = data.asInstanceOf[Seq[Any]]
    val subRow = Row.fromSeq(seq)
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    seq.indices.foreach { idx =>
      val arrayField = StructField(idx.toString, elementType)
      fillBufferPartiallyApplied(arrayField, subRow(idx), Some(qualifiedName))
    }
  }

  private def fillMap(
      valueType: DataType,
      flatSchemaIndexes: Map[String, Int],
      buffer: ArrayBuffer[Any],
      data: Any,
      qualifiedName: String): Unit = {
    val map = data.asInstanceOf[Map[Any, Any]]
    val subRow = Row.fromSeq(map.values.toSeq)
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    map.keys.zipWithIndex.foreach { case (key, idx) =>
      val mapField = StructField(key.toString, valueType)
      fillBufferPartiallyApplied(mapField, subRow(idx), Some(qualifiedName))
    }
  }

  private def fillStruct(
      fields: Seq[StructField],
      flatSchemaIndexes: Map[String, Int],
      buffer: ArrayBuffer[Any],
      data: Any,
      qualifiedName: String): Unit = {
    val subRow = data.asInstanceOf[Row]
    val fillBufferPartiallyApplied = fillBuffer(flatSchemaIndexes, buffer) _
    fields.zipWithIndex.foreach { case (subField, idx) =>
      fillBufferPartiallyApplied(subField, subRow(idx), Some(qualifiedName))
    }
  }

  def flattenSchema(df: DataFrame): StructType = {
    val rowSchemas = rowsToRowSchemas(df)
    val mergedSchema = mergeRowSchemas(rowSchemas)
    StructType(mergedSchema.map(_.field))
  }

  private def rowsToRowSchemas(df: DataFrame): Dataset[Seq[FieldWithOrder]] = {
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[Seq[FieldWithOrder]]
    val originalSchema = df.schema
    df.map[Seq[FieldWithOrder]] { row: Row =>
      originalSchema.fields.zipWithIndex.foldLeft(Seq.empty[FieldWithOrder]) {
        case (acc, (field, index)) => acc ++ flattenField(field, row(index), index :: Nil)
      }
    }
  }

  private def mergeRowSchemas(ds: Dataset[Seq[FieldWithOrder]]): Seq[FieldWithOrder] = ds.reduce {
    (first, second) =>
      val firstMap = convertRowSchemaToPathToFieldMap(first)
      val secondMap = convertRowSchemaToPathToFieldMap(second)
      val keys = (firstMap.keySet ++ secondMap.keySet).toSeq.sorted(fieldPathOrdering)
      keys.map { key =>
        (firstMap.get(key), secondMap.get(key)) match {
          case (None, Some(StructField(name, dataType, _, _))) =>
            FieldWithOrder(StructField(name, dataType, nullable = true), key)
          case (Some(StructField(name, dataType, _, _)), None) =>
            FieldWithOrder(StructField(name, dataType, nullable = true), key)
          case (Some(StructField(name, dataType, nullable1, _)), Some(StructField(_, _, nullable2, _))) =>
            FieldWithOrder(StructField(name, dataType, nullable1 || nullable2), key)
          case (None, None) =>
            throw new IllegalStateException(s"There must be a corresponding value for key '$key' in one map at least.")
        }
      }
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

  private def convertRowSchemaToPathToFieldMap(rowSchema: Seq[FieldWithOrder]): Map[Seq[Any], StructField] = {
    rowSchema.map(f => f.order -> f.field).toMap
  }

  private def getQualifiedName(prefix: Option[String], name: String): String = prefix match {
    case None => name
    case Some(p) => s"${p}_$name"
  }

  private def flattenField(
      originalField: StructField,
      data: Any,
      path: List[Any],
      prefix: Option[String] = None,
      isParentNullable: Boolean = false): Seq[FieldWithOrder] = {
    if (data != null) {
      val StructField(name, dataType, nullable, _) = originalField
      val qualifiedName = getQualifiedName(prefix, name)
      val nullableField = isParentNullable || nullable
      dataType match {
        case BinaryType =>
          flattenArrayType(ByteType, containsNull = false, data, path, qualifiedName, nullableField = nullableField)
        case MapType(_, valueType, containsNull) =>
          flattenMapType(valueType, containsNull, data, path, qualifiedName, nullableField)
        case ArrayType(elementType, containsNull) =>
          flattenArrayType(elementType, containsNull, data, path, qualifiedName, nullableField)
        case StructType(fields) =>
          flattenStructType(fields, data, path, qualifiedName, nullableField)
        case dt =>
          FieldWithOrder(StructField(qualifiedName, dt, nullableField), path.reverse) :: Nil
      }
    } else {
      Nil
    }
  }

  private case class FieldWithOrder(field: StructField, order: Seq[Any])

  private def flattenArrayType(
      elementType: DataType,
      containsNull: Boolean,
      data: Any,
      path: List[Any],
      qualifiedName: String,
      nullableField: Boolean) = {
    val values = data.asInstanceOf[Seq[Any]]
    val subRow = Row.fromSeq(values)
    values.indices.flatMap { idx =>
      val arrayField = StructField(idx.toString(), elementType, containsNull)
      flattenField(arrayField, subRow(idx), idx :: path, Some(qualifiedName), nullableField)
    }
  }

  private def flattenMapType(
      valueType: DataType,
      containsNull: Boolean,
      data: Any,
      path: List[Any],
      qualifiedName: String,
      nullableField: Boolean) = {
    val map = data.asInstanceOf[Map[Any, Any]]
    val subRow = Row.fromSeq(map.values.toSeq)
    map.keys.zipWithIndex.flatMap { case (key, idx) =>
      val mapField = StructField(key.toString, valueType, containsNull)
      flattenField(mapField, subRow(idx), key :: path, Some(qualifiedName), nullableField)
    }.toSeq
  }

  private def flattenStructType(
      fields: Seq[StructField],
      data: Any,
      path: List[Any],
      qualifiedName: String,
      nullableField: Boolean) = {
    val subRow = data.asInstanceOf[Row]
    fields.zipWithIndex.flatMap { case (subField, idx) =>
      flattenField(subField, subRow(idx), idx :: path, Some(qualifiedName), nullableField)
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
        case ArrayType(arrType, nullable) =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, arrType, nullable)
          }
        case BinaryType =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx, ByteType, nullable = false)
          }
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
    * @param flatDataFrame flat data frame
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
        case ArrayType(_, _) =>
          Array.fill(elemMaxSizes(idx))(false).toSeq
        case BinaryType =>
          Array.fill(elemMaxSizes(idx))(false).toSeq
        case _: ml.linalg.VectorUDT | _: mllib.linalg.VectorUDT =>
          Array.fill(elemMaxSizes(idx))(sparseInfoForVec(idx)).toSeq
        case _ => Seq(false)
      }
    }
  }


  def expandWithoutVectors(flatSchema: StructType, elemMaxSizes: Array[Int]): Seq[StructField] = {
    val expandedSchema = flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case ArrayType(arrType, nullable) =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, arrType, nullable)
          }
        case BinaryType =>
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, ByteType, nullable = false)
          }
        case _ => Seq(field)
      }
    }
    expandedSchema
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
    val arrayIndices = collectArrayLikeTypes(flatDataFrame.schema)
    val vectorIndices = collectVectorLikeTypes(flatDataFrame.schema)
    val simpleIndices = collectSimpleLikeTypes(flatDataFrame.schema)
    val collectionIndices = arrayIndices ++ vectorIndices

    val sizeFromMetadata = collectionIndices.map(idx => fieldSizeFromMetadata(flatDataFrame.schema.fields(idx)))
    val maxCollectionSizes = if (sizeFromMetadata.forall(_.isDefined)) {
      sizeFromMetadata.map(_.get).toArray
    } else {
      val sizes = flatDataFrame.rdd.map { row =>
        collectionIndices.map { idx => getCollectionSize(row, idx) }
      }
      if (sizes.isEmpty) {
        Array(0)
      } else {
        sizes.reduce((a, b) => a.indices.map(i => if (a(i) > b(i)) a(i) else b(i))).toArray
      }
    }

    val collectionIdxToSize = collectionIndices.zip(maxCollectionSizes).toMap
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
        case ArrayType(_, _) => row.getAs[Seq[_]](idx).length
        case BinaryType => row.getAs[Array[Byte]](idx).length
        case _: ml.linalg.VectorUDT => row.getAs[ml.linalg.Vector](idx).size
        case _: mllib.linalg.VectorUDT => row.getAs[mllib.linalg.Vector](idx).size
        case udt: UserDefinedType[_] => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
      }
    }
  }

}
