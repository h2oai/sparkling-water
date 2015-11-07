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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.H2OSchemaUtils.vecTypeToDataType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import water.fvec.H2OFrame
import water.parser.BufferedString

/**
 * H2O H2OFrame wrapper providing RDD[Row] API.
 *
 * @param h2oContext
 * @param frame
 */
private[spark]
class H2OSchemaRDD(@transient val h2oContext: H2OContext,
                   @transient val frame: H2OFrame)
  extends RDD[InternalRow](h2oContext.sparkContext, Nil) with H2ORDDLike {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val kn = keyName

    new H2OChunkIterator[InternalRow] {
      override val partIndex: Int = split.index
      override val keyName: String = kn

      /** Mutable row returned by iterator */
      val mutableRow = new GenericMutableRow(ncols)
      /** Dummy muttable holder for String values */
      val valStr = new BufferedString()
      /** Types for of columns */
      // FIXME: should be cached
      lazy val types = fr.vecs().map( v => vecTypeToDataType(v))

      override def next(): InternalRow = {
        var i = 0
        while (i < ncols) {
          val chk = chks(i)
          val typ = types(i)
          if (chk.isNA(row)) {
            mutableRow.setNullAt(i)
          } else {
            typ match {
              case ByteType =>
                mutableRow.setByte(i, chk.at8(row).asInstanceOf[Byte])
              case ShortType =>
                mutableRow.setShort(i, chk.at8(row).asInstanceOf[Short])
              case IntegerType =>
                mutableRow.setInt(i, chk.at8(row).asInstanceOf[Int])
              case LongType =>
                mutableRow.setLong(i, chk.at8(row))
              case FloatType =>
                mutableRow.setFloat(i, chk.atd(row).asInstanceOf[Float])
              case DoubleType =>
                mutableRow.setDouble(i, chk.atd(row))
              case BooleanType =>
                mutableRow.setBoolean(i, chk.at8(row) == 1)
              case StringType =>
                val utf8 = if (chk.vec().isCategorical) {
                  val str = chk.vec().domain()(chk.at8(row).asInstanceOf[Int])
                  UTF8String.fromString(str)
                } else if (chk.vec().isString) {
                  chk.atStr(valStr, row)
                  UTF8String.fromString(valStr.toString) // TODO improve this.
                } else if (chk.vec().isUUID) {
                  val uuid = new UUID(chk.at16h(row), chk.at16l(row))
                  UTF8String.fromString(uuid.toString)
                } else null
                mutableRow.update(i, utf8)
              case TimestampType =>
                mutableRow.setLong(i, chk.at8(row) * 1000L)
              case _ => ???
            }
          }
          i += 1
        }
        row += 1
        // Return result
        mutableRow
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val num = frame.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }
}
