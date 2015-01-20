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

import org.apache.spark.sql.catalyst.types._
import water.fvec.Vec

/**
 * Utilities for working with Spark SQL component.
 */
object H2OSchemaUtils {

  def createSchema(f: DataFrame): StructType = {
    val types = new Array[StructField](f.numCols())
    val vecs = f.vecs()
    val names = f.names()
    for (i <- 0 until f.numCols()) {
      val vec = vecs(i)
      types(i) = StructField(
        names(i), // Name of column
        h2oTypeToDataType(vec), // Catalyst type of column
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
  def h2oTypeToDataType(v: Vec): DataType = {
    v.get_type() match {
      case Vec.T_NUM  => h2oNumericTypeToDataType(v)
      case Vec.T_ENUM => StringType
      case Vec.T_UUID => StringType
      case Vec.T_STR  => StringType
      case typ => if (typ>=Vec.T_TIME && typ<=Vec.T_TIMELAST) TimestampType
                  else ???
    }
  }

  def h2oNumericTypeToDataType(v: Vec): DataType = {
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
}
