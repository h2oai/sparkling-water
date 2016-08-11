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

package org.apache.spark.rdd

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OSchemaUtils.vecTypeToDataType
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, GenericRow}
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.Frame
import water.parser.BufferedString


/**
  * H2O H2OFrame wrapper providing RDD[Row] API for use in BaseRelation API.
  *
  * Note: keep this code up-to-date with H2OSchemaRDDInternal
  *
  * @param frame  an instance of H2O frame
  * @param requiredColumns  list of the columns which should be provided by iterator, null means all
  * @param sparkContext  a running spark context
  */
private[spark]
class H2OSchemaRDD[T <: Frame](@transient val frame: T,
                               val requiredColumns: Array[String])
                              (@transient sparkContext: SparkContext)
  extends RDD[Row](sparkContext, Nil) with H2ORDDLike[T] {

  def this(@transient frame: T)
          (@transient sparkContext: SparkContext) = this(frame, null)(sparkContext)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val kn = frameKeyName

    new H2OChunkIterator[Row] {
      override val partIndex: Int = split.index
      override val keyName: String = kn

      /** Dummy muttable holder for String values */
      val valStr = new BufferedString()
      /* Indexes of selected columns */
      val selectedColumnIndices = if (requiredColumns == null) {
        fr.names().indices
      } else {
        val names = fr.names()
        names.indices.filter { idx =>
          requiredColumns.contains(names(idx))
        }
      }
      // scalastyle:off
      assert(if (requiredColumns == null)
               selectedColumnIndices.length == fr.numCols()
             else
               selectedColumnIndices.length == requiredColumns.length,
        "Column selection missing a column!"
      )
      // scalastyle:on
      /* Types for of columns */
      lazy val types = fr.vecs().map(v => vecTypeToDataType(v))

      /** Mutable row returned by iterator */
      val mutableRow = new Array[Any](selectedColumnIndices.length)

      override def next(): Row = {
        selectedColumnIndices.foreach { i =>
          val chk = chks(i)
          val vec = chk.vec()
          val typ = types(i)
          if (chk.isNA(row)) {
            mutableRow(i) = null
          } else {
            typ match {
              case ByteType =>
                mutableRow(i) = chk.at8(row).toByte
              case ShortType =>
                mutableRow(i) = chk.at8(row).toShort
              case IntegerType =>
                mutableRow(i) = chk.at8(row).toInt
              case LongType =>
                mutableRow(i) = chk.at8(row)
              case FloatType =>
                mutableRow(i) = chk.atd(row).toFloat
              case DoubleType =>
                mutableRow(i) = chk.atd(row)
              case BooleanType =>
                mutableRow(i) = chk.at8(row) == 1
              case StringType =>
                val utf8 = if (vec.isCategorical) {
                  val str = vec.domain()(chk.at8(row).toInt)
                  UTF8String.fromString(str)
                } else if (vec.isString) {
                  chk.atStr(valStr, row)
                  UTF8String.fromString(valStr.toString) // TODO improve this.
                } else if (vec.isUUID) {
                  val uuid = new UUID(chk.at16h(row), chk.at16l(row))
                  UTF8String.fromString(uuid.toString)
                } else null
                mutableRow(i) = utf8
              case TimestampType =>
                mutableRow(i) = chk.at8(row) * 1000L
              case _ => ???
            }
          }
                                      }
        row += 1
        // Return result
        new GenericRow(mutableRow)
      }
    }
  }
}

/**
 * H2O H2OFrame wrapper providing RDD[InternalRow] API.
 *
 * @param frame  an H2O frame
 * @param requiredColumns  list of the columns which should be provided by iterator, null means all
 * @param sparkContext a running Spark context
 */
private[spark]
class H2OSchemaRDDInternal[T <: water.fvec.Frame](@transient val frame: T,
                                          val requiredColumns: Array[String])
                                         (@transient sparkContext: SparkContext)
  extends RDD[InternalRow](sparkContext, Nil) with H2ORDDLike[T] {

  def this(@transient frame: T)
          (@transient sparkContext: SparkContext) = this(frame, null)(sparkContext)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val kn = frameKeyName

    new H2OChunkIterator[InternalRow] {
      override val partIndex: Int = split.index
      override val keyName: String = kn

      /** Dummy muttable holder for String values */
      val valStr = new BufferedString()
      /* Indexes of selected columns */
      val selectedColumnIndices = if (requiredColumns == null) {
        fr.names().indices
      } else {
        requiredColumns.toSeq.map { name =>
          fr.find(name)
        }
      }
      // Make sure that column selection is consistent
      // scalastyle:off
      assert(if (requiredColumns == null)
               selectedColumnIndices.length == fr.numCols()
             else
               selectedColumnIndices.length == requiredColumns.length,
             "Column selection missing a column!"
      )
      // scalastyle:on
      /** Types for of columns */
      lazy val types = fr.vecs().map( v => vecTypeToDataType(v))
      /** Mutable row returned by iterator */
      val mutableRow = new GenericMutableRow(selectedColumnIndices.length)

      override def next(): InternalRow = {
        selectedColumnIndices.indices.foreach { idx =>
          val i = selectedColumnIndices(idx)
          val chk = chks(i)
          val typ = types(i)
          val vec = chk.vec()
          if (chk.isNA(row)) {
            mutableRow.setNullAt(idx)
          } else {
            typ match {
              case ByteType =>
                mutableRow.setByte(idx, chk.at8(row).asInstanceOf[Byte])
              case ShortType =>
                mutableRow.setShort(idx, chk.at8(row).asInstanceOf[Short])
              case IntegerType =>
                mutableRow.setInt(idx, chk.at8(row).asInstanceOf[Int])
              case LongType =>
                mutableRow.setLong(idx, chk.at8(row))
              case FloatType =>
                mutableRow.setFloat(idx, chk.atd(row).asInstanceOf[Float])
              case DoubleType =>
                mutableRow.setDouble(idx, chk.atd(row))
              case BooleanType =>
                mutableRow.setBoolean(idx, chk.at8(row) == 1)
              case StringType =>
                val utf8 = if (vec.isCategorical) {
                  val str = vec.domain()(chk.at8(row).asInstanceOf[Int])
                  UTF8String.fromString(str)
                } else if (vec.isString) {
                  chk.atStr(valStr, row)
                  UTF8String.fromString(valStr.toString)
                } else if (vec.isUUID) {
                  val uuid = new UUID(chk.at16h(row), chk.at16l(row))
                  UTF8String.fromString(uuid.toString)
                } else null
                mutableRow.update(idx, utf8)
              case TimestampType =>
                mutableRow.setLong(idx, chk.at8(row) * 1000L)
              case _ => ???
            }
          }
        }
        row += 1
        // Return result
        mutableRow
      }
    }
  }
}
