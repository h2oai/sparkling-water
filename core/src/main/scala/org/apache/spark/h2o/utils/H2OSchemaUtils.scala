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
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, ml, mllib}

/**
 * Utilities for working with Spark SQL component.
 */
object H2OSchemaUtils {
  import ReflectionUtils._

  val NORMAL_TYPE : Byte = 0
  val ARRAY_TYPE : Byte  = 1
  val VEC_TYPE : Byte = 2

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

  /** Return flattenized type - recursively transforms StrucType into Seq of encapsulated types. */
  def flatSchema(s: StructType, typeName: Option[String] = None, nullable: Boolean = false): Seq[StructField] = {
    s.fields.flatMap(f =>
      f.dataType match {
        case struct: StructType =>
          flatSchema(struct,
            typeName.map(s => s"$s${f.name}.").orElse(Option(s"${f.name}.")),
            nullable || f.nullable)
        case simple: DataType =>
          Seq(StructField(
            typeName.map(n => s"$n${f.name}").getOrElse(f.name),
            simple,
            f.nullable || nullable))
      })
  }

  /** Returns expanded schema
    *  - schema is represented as list of types and its position inside row
    *  - all arrays are expanded into columns based on the longest one
    *  - all vectors are expanded into columns
    *
    * @param sc  actual Spark context
    * @param srdd  schema-based RDD
    * @return list of types with their positions
    */
  def expandedSchema(sc: SparkContext, srdd: DataFrame): Seq[(Seq[Int], StructField, Byte)] = {
    val schema: StructType = srdd.schema
    // Collect max size in array and vector columns to expand them
    val arrayColIdxs  = collectArrayLikeTypes(schema.fields)
    val vecColIdxs    = collectVectorLikeTypes(schema.fields)
    val numOfArrayCols = arrayColIdxs.length
    // Collect max arrays for this RDD, it is distributed operation
    val fmaxLens = collectMaxArrays(sc, srdd.rdd, arrayColIdxs, vecColIdxs)
    // Flattens RDD's schema
    val flatRddSchema = flatSchema(schema)
    val typeIndx = collectTypeIndx(schema.fields)
    val typesAndPath = typeIndx
                .zip(flatRddSchema) // Seq[(Seq[Int], StructField)]
    var arrayCnt = 0; var vecCnt = 0
    // Generate expanded schema
    val expSchema = typesAndPath.indices.flatMap {idx =>
      val tap = typesAndPath(idx)
      val path = tap._1
      val field = tap._2
      field.dataType match {
        case ArrayType(aryType,nullable) =>
          val result = (0 until fmaxLens(arrayCnt)).map(i =>
            (path, StructField(field.name + i.toString, aryType, nullable), ARRAY_TYPE)
          )
          arrayCnt += 1
          result

        case t if t.isInstanceOf[UserDefinedType[_]] =>
          // t.isInstanceOf[UserDefinedType[mllib.linalg.Vector]]
          val result = (0 until fmaxLens(numOfArrayCols + vecCnt)).map(i =>
            (path, StructField(field.name + i.toString, DoubleType, nullable = true), VEC_TYPE)
          )
          vecCnt += 1
          result

        case _ => Seq((path, field, NORMAL_TYPE))
      }
    }
    assert(arrayCnt == numOfArrayCols)
    assert(vecCnt == vecColIdxs.length)
    // Return result
    expSchema
  }

  def collectTypeIndx(fields: Seq[StructField], path: Seq[Int] = Seq()): Seq[Seq[Int]] = {
    fields.indices.flatMap(i => fields(i).dataType match {
      case StructType(fs) => collectTypeIndx(fs, path++Seq(i))
      case _  => Seq(path++Seq(i))
    })
  }

  /** Collect all StringType indexes in give list representing schema
    */
  def collectStringTypesIndx(fields: Seq[StructField], path: Seq[Int] = Seq()): Seq[Seq[Int]] = {
    fields.indices.flatMap(i => fields(i).dataType match {
      case StructType(fs) => collectStringTypesIndx(fs, path++Seq(i))
      case StringType  => Seq(path++Seq(i))
      case _ => Nil
    })
  }

  def collectArrayLikeTypes(fields: Seq[StructField], path: Seq[Int] = Seq()): Seq[Seq[Int]] = {
    fields.indices.flatMap(i => fields(i).dataType match {
      case StructType(fs) => collectArrayLikeTypes(fs, path++Seq(i))
      case ArrayType(_,_)  => Seq(path++Seq(i))
      case _ => Nil
    })
  }

  def collectVectorLikeTypes(fields: Seq[StructField], path: Seq[Int] = Seq()): Seq[Seq[Int]] = {
    fields.indices.flatMap(i => fields(i).dataType match {
      case StructType(fs) => collectVectorLikeTypes(fs, path++Seq(i))
      case t => if (t.isInstanceOf[UserDefinedType[_/*mllib.linalg.Vector*/]]) Seq(path++Seq(i)) else Nil
    })
  }

  /** Collect max size of stored arrays and MLLib vectors.
 *
    * @return list of max sizes for array types, followed by max sizes for vector types. */
  private[h2o]
  def collectMaxArrays(sc: SparkContext,
                       rdd: RDD[Row],
                       arrayTypesIndx: Seq[Seq[Int]],
                       vectorTypesIndx: Seq[Seq[Int]]): Array[Int] = {
    val allTypesIndx = arrayTypesIndx ++ vectorTypesIndx
    val numOfArrayTypes = arrayTypesIndx.length
    val maxvec = rdd.map(row => {
      val acc = new Array[Int](allTypesIndx.length)
      allTypesIndx.indices.foreach { k =>
        val indx = allTypesIndx(k)
        var i = 0
        var subRow = row
        while (i < indx.length-1 && !subRow.isNullAt(indx(i))) { subRow = subRow.getAs[Row](indx(i)); i += 1 }
        if (!subRow.isNullAt(indx(i))) {
          val olen = if (k < numOfArrayTypes) { // it is array
            subRow.getAs[Seq[_]](indx(i)).length
          } else { // it is user defined type - we support vectors now only
            val value = subRow.get(indx(i))
            value match {
              case _: mllib.linalg.Vector =>
                subRow.getAs[mllib.linalg.Vector](indx(i)).size
              case _: ml.linalg.Vector =>
                subRow.getAs[ml.linalg.Vector](indx(i)).size
              case _ =>
                throw new UnsupportedOperationException(s"User defined type is not supported: ${value.getClass}")
            }
          }
          // Set max
          if (olen > acc(k)) acc(k) = olen
        }
      }
      acc
    }).reduce((a,b) => a.indices.map(i => if (a(i) > b(i)) a(i) else b(i)).toArray)
    // Result
    maxvec
  }
}
