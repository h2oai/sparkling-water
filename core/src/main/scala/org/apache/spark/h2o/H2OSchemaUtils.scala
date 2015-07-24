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

package org.apache.spark.h2o

import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, UserDefinedType}
import org.apache.spark.{SparkContext, mllib}
import water.fvec.Vec
import water.parser.Categorical

import scala.collection.mutable

/**
 * Utilities for working with Spark SQL component.
 */
object H2OSchemaUtils {

  val NORMAL_TYPE : Byte = 0
  val ARRAY_TYPE : Byte  = 1
  val VEC_TYPE : Byte = 2

  def createSchema(f: H2OFrame): StructType = {
    val types = new Array[StructField](f.numCols())
    val vecs = f.vecs()
    val names = f.names()
    for (i <- 0 until f.numCols()) {
      val vec = vecs(i)
      types(i) = StructField(
        names(i), // Name of column
        vecTypeToDataType(vec), // Catalyst type of column
        vec.naCnt() > 0
      )
    }
    StructType(types)
  }

  /**
   * Return catalyst structural type for given H2O vector.
   *
   * The mapping of type is flat, if type is unrecognized
   * {@link IllegalArgumentException} is thrown.
   *
   * @param v H2O vector
   * @return catalyst data type
   */
  def vecTypeToDataType(v: Vec): DataType = {
    v.get_type() match {
      case Vec.T_BAD  => ByteType // vector is full of NAs, use any type
      case Vec.T_NUM  => numericVecTypeToDataType(v)
      case Vec.T_ENUM => StringType
      case Vec.T_UUID => StringType
      case Vec.T_STR  => StringType
      case Vec.T_TIME => TimestampType
      case typ => throw new IllegalArgumentException("Unknown vector type " + typ)
    }
  }

  def numericVecTypeToDataType(v: Vec): DataType = {
    if (v.isInt) {
      val min = v.min()
      val max = v.max()
      if (min > Byte.MinValue && max < Byte.MaxValue) {
        ByteType
      } else if (min > Short.MinValue && max < Short.MaxValue) {
        ShortType
      } else if (min > Int.MinValue && max < Int.MaxValue) {
        IntegerType
      } else {
        LongType
      }
    } else DoubleType
  }

  /** Method translating SQL types into Sparkling Water types */
  def dataTypeToVecType(dt : DataType, d: Array[String]) : Byte = dt match {
    case BinaryType  => Vec.T_NUM
    case ByteType    => Vec.T_NUM
    case ShortType   => Vec.T_NUM
    case IntegerType => Vec.T_NUM
    case LongType    => Vec.T_NUM
    case FloatType   => Vec.T_NUM
    case DoubleType  => Vec.T_NUM
    case BooleanType => Vec.T_NUM
    case TimestampType => Vec.T_TIME
    case StringType  => if (d!=null && d.length < water.parser.Categorical.MAX_ENUM_SIZE) {
                          Vec.T_ENUM
                        } else {
                          Vec.T_STR
                        }
    //case StructType  => dt.
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
  }

  /** Return flattenized type - recursively transforms StrucType into Seq of encapsulated types. */
  def flatSchema(s: StructType, typeName: Option[String] = None, nullable: Boolean = false): Seq[StructField] = {
    s.fields.flatMap(f =>
      if (f.dataType.isInstanceOf[StructType])
        flatSchema(f.dataType.asInstanceOf[StructType],
          typeName.map(s => s"$s${f.name}.").orElse(Option(s"${f.name}.")),
          nullable || f.nullable)
      else
        Seq(StructField(
          typeName.map(n => s"$n${f.name}").getOrElse(f.name),
          f.dataType,
          f.nullable || nullable)))
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
    // Collect max size in array and vector columns to expand them
    val arrayColIdxs  = collectArrayLikeTypes(srdd.schema.fields)
    val vecColIdxs    = collectVectorLikeTypes(srdd.schema.fields)
    val numOfArrayCols = arrayColIdxs.length
    // Collect max arrays for this RDD, it is distributed operation
    val fmaxLens = collectMaxArrays(sc, srdd.rdd, arrayColIdxs, vecColIdxs)
    // Flattens RDD's schema
    val flatRddSchema = flatSchema(srdd.schema)
    val typeIndx = collectTypeIndx(srdd.schema.fields)
    val typesAndPath = typeIndx
                .zip(flatRddSchema) // Seq[(Seq[Int], StructField)]
    var arrayCnt = 0; var vecCnt = 0
    // Generate expanded schema
    val expSchema = typesAndPath.indices.flatMap {idx =>
      val tap = typesAndPath(idx)
      val path = tap._1
      val field = tap._2
      field.dataType match {
        case ArrayType(aryType,nullable) => {
          val result = (0 until fmaxLens(arrayCnt)).map(i =>
            (path, StructField(field.name+i.toString, aryType, nullable), ARRAY_TYPE)
          )
          arrayCnt += 1
          result
        }
        case t if t.isInstanceOf[UserDefinedType[_]] => {
          // t.isInstanceOf[UserDefinedType[mllib.linalg.Vector]]
          val result = (0 until fmaxLens(numOfArrayCols + vecCnt)).map(i =>
            (path, StructField(field.name+i.toString, DoubleType, true), VEC_TYPE)
          )
          vecCnt += 1
          result
        }
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

  private[h2o]
  def collectColumnDomains(sc: SparkContext,
                            rdd: RDD[Row],
                            stringTypesIdx: Seq[Seq[Int]]): Array[Array[String]] = {
    // Create accumulable collections for each possible string variable
    val accs = stringTypesIdx.indices.map( _ => sc.accumulableCollection(new mutable.HashSet[String]()))
    // TODO: perform via partition, indicates string columns and fail early
    rdd.foreach { r => { // row
      // Update accumulable variables
      stringTypesIdx.indices.foreach { k => {
        val indx = stringTypesIdx(k)
        val acc = accs(k)
        var i = 0
        var subRow = r
        while (i < indx.length-1 && !subRow.isNullAt(indx(i))) { subRow = subRow.getAs[Row](indx(i)); i += 1 }
        if (!subRow.isNullAt(indx(i))) acc += subRow.getString(indx(i))
      }
      }
    }
    }
    // Domain for each enum column or null
    accs.map(acc => if (acc.value.size > Categorical.MAX_ENUM_SIZE) null else acc.value.toArray.sorted).toArray
  }

  /** Collect max size of stored arrays and MLLib vectors.
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
          } else { // it is vector
            subRow.getAs[mllib.linalg.Vector](indx(i)).size
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
