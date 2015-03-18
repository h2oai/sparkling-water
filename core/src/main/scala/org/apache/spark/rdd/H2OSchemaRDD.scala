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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OSchemaUtils.vecTypeToDataType
import org.apache.spark.h2o.{H2OSchemaUtils, H2OContext}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.{Partition, TaskContext}
import water.fvec.DataFrame
import water.parser.ValueString

/**
 * H2O DataFrame wrapper providing RDD[Row] API.
 *
 * @param h2oContext
 * @param frame
 */
private[spark]
class H2OSchemaRDD(@transient val h2oContext: H2OContext,
                   @transient val frame: DataFrame)
  extends RDD[Row](h2oContext.sparkContext, Nil) with H2ORDDLike {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val kn = keyName

    new H2OChunkIterator[Row] {
      override val partIndex: Int = split.index
      override val keyName: String = kn

      /** Mutable row returned by iterator */
      val mutableRow = new GenericMutableRow(ncols)
      /** Dummy muttable holder for String values */
      val valStr = new ValueString()
      /** Types for of columns */
      // FIXME: should be cached
      lazy val types = fr.vecs().map( v => vecTypeToDataType(v))

      override def next(): Row = {
        var i = 0
        while (i < ncols) {
          val chk = chks(i)
          val typ = types(i)
          if (chk.isNA(row)) {
            mutableRow.setNullAt(i)
          } else {
            mutableRow(i) = typ match {
              case ByteType => chk.at8(row).asInstanceOf[Byte]
              case ShortType => chk.at8(row).asInstanceOf[Short]
              case IntegerType => chk.at8(row).asInstanceOf[Int]
              case FloatType => chk.atd(row)
              case DoubleType => chk.atd(row)
              case BooleanType => chk.at8(row) == 1
              case StringType =>
                if (chk.vec().isEnum) {
                  chk.vec().domain()(chk.at8(row).asInstanceOf[Int])
                } else if (chk.vec().isString) {
                  chk.atStr(valStr, row)
                  valStr.toString
                } else None
              case TimestampType => new java.sql.Timestamp(chk.at8(row))
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
