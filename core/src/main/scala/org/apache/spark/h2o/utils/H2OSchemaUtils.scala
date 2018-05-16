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
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, ml, mllib}

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

  def flattenSchema(schema: StructType, prefix: String = null, nullable: Boolean = false): StructType = {

    val flattened = schema.fields.flatMap { f =>
      val escaped = if (f.name.contains(".")) "`" + f.name + "`" else f.name
      val colName = if (prefix == null) escaped else prefix + "." + escaped

      f.dataType match {
        case st: StructType => flattenSchema(st, colName, nullable || f.nullable)
        case _ => Array[StructField](StructField(colName, f.dataType, nullable || f.nullable))
      }
    }
    StructType(flattened)
  }

  def flattenDataFrame(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.col
    val flatten = flattenSchema(df.schema)
    val cols = flatten.map(f => col(f.name).as(f.name.replaceAll("`", "")))
    df.select(cols: _*)
  }


  /** Returns expanded schema
    *  - schema is represented as list of types
    *  - all arrays are expanded into columns based on the longest one
    *  - all vectors are expanded into columns based on the longest one
    *
    * @param sc         Spark context
    * @param flatSchema flat schema of spark data frame
    * @return list of types with their positions
    */
  def expandedSchema(sc: SparkContext, flatSchema: StructType, elemMaxSizes: Array[Int]): Seq[StructField] = {

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
        case t if t.isInstanceOf[UserDefinedType[_]] =>
          // t.isInstanceOf[UserDefinedType[mllib.linalg.Vector]]
          (0 until elemMaxSizes(idx)).map { arrIdx =>
            StructField(field.name + arrIdx.toString, DoubleType, nullable = true)
          }
        case _ => Seq(field)
      }
    }
    expandedSchema
  }

  def expandWithoutVectors(sc: SparkContext, flatSchema: StructType, elemMaxSizes: Array[Int]): Seq[StructField] = {
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
    if(maxElemSizes.length == 0){
      startPositions
    }else {
      startPositions(0) = 0
      (1 until maxElemSizes.length).foreach { idx =>
        startPositions(idx) = startPositions(idx - 1) + maxElemSizes(idx - 1)
      }
      startPositions
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
  def collectMaxElementSizes(sc: SparkContext, flatDataFrame: DataFrame): Array[Int] = {
    val arrayIndices = collectArrayLikeTypes(flatDataFrame.schema)
    val vectorIndices = collectVectorLikeTypes(flatDataFrame.schema)
    val simpleIndices = collectSimpleLikeTypes(flatDataFrame.schema)
    val collectionIndices = arrayIndices ++ vectorIndices

    val attributesNum = (field: StructField) => {
      if (!field.dataType.isInstanceOf[ml.linalg.VectorUDT]) {
        None
      }
      else if (AttributeGroup.fromStructField(field).size != -1) {
        Some(AttributeGroup.fromStructField(field).size)
      }
      else {
        None
      }
    }


    val sizeFromMetadata = collectionIndices.map(idx => attributesNum(flatDataFrame.schema.fields(idx)))
    val maxCollectionSizes = if (sizeFromMetadata.forall(_.isDefined)) {
      sizeFromMetadata.map(_.get).toArray
    } else {
      flatDataFrame.rdd.map { row =>
        collectionIndices.map { idx => getCollectionSize(row, idx) }
      }.reduce((a, b) => a.indices.map(i => if (a(i) > b(i)) a(i) else b(i))).toArray
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
        case StringType => Option(idx)
        case _ => None
      }
    }
  }

  def collectArrayLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case ArrayType(_, _) => Option(idx)
        case BinaryType => Option(idx)
        case _ => None
      }
    }
  }

  def collectVectorLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case _: UserDefinedType[_ /*mllib.linalg.Vector*/ ] => Option(idx)
        case _ => None
      }
    }
  }

  private def collectSimpleLikeTypes(flatSchema: StructType): Seq[Int] = {
    flatSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        case BooleanType => Option(idx)
        case ByteType => Option(idx)
        case ShortType => Option(idx)
        case IntegerType => Option(idx)
        case LongType => Option(idx)
        case FloatType => Option(idx)
        case _: DecimalType => Option(idx)
        case DoubleType => Option(idx)
        case StringType => Option(idx)
        case TimestampType => Option(idx)
        case DateType => Option(idx)
        case _ => None
      }
    }
  }


  /**
    * Get size of Array or Vector type. Already expects flat DataFrame
    *
    * @param row current row
    * @param idx index of the element
    */
  private def getCollectionSize(row: Row, idx: Int): Int = {
    if (row.isNullAt(idx)) {
      return 0
    }

    val elemType = row.schema.fields(idx)
    elemType.dataType match {
      case ArrayType(_, _) => row.getAs[Seq[_]](idx).length
      case BinaryType => row.getAs[Array[Byte]](idx).length
      // it is user defined type - currently, only vectors are supported
      case _ =>
        val value = row.get(idx)
        value match {
          case _: mllib.linalg.Vector =>
            row.getAs[mllib.linalg.Vector](idx).size
          case _: ml.linalg.Vector =>
            row.getAs[ml.linalg.Vector](idx).size
          case _ =>
            throw new UnsupportedOperationException(s"User defined type is not supported: ${value.getClass}")
        }
    }
  }

}
