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

import org.apache.spark.SparkContext
import water.fvec.Vec
import water.parser.Categorical

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Utilities to work with primitive types such as String, Integer, Double..
 *
 */
object H2OPrimitiveTypesUtils {

  /** Method used for obtaining column domains */
  private[spark]
  def collectColumnDomains[T](sc: SparkContext,
                              rdd: RDD[T],
                              fnames: Array[String],
                              ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc = sc.accumulableCollection(new mutable.HashSet[String]())
      rdd.foreach(r => {
        acc += r.asInstanceOf[String]
      })
      res(idx) = if (acc.value.size > Categorical.MAX_CATEGORICAL_COUNT) null else acc.value.toArray.sorted
    }
    res
  }

  /** Method translating primitive types into Sparkling Water types
    * This method is already prepared to handle all mentioned primitive types  */
  private[spark]
  def dataTypeToVecType(t: Class[_], d: Array[String]): Byte = {
    t match {
      case q if q == classOf[java.lang.Byte] => Vec.T_NUM
      case q if q == classOf[java.lang.Short] => Vec.T_NUM
      case q if q == classOf[java.lang.Integer] => Vec.T_NUM
      case q if q == classOf[java.lang.Long] => Vec.T_NUM
      case q if q == classOf[java.lang.Float] => Vec.T_NUM
      case q if q == classOf[java.lang.Double] => Vec.T_NUM
      case q if q == classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q == classOf[java.lang.String] => if (d != null && d.length < water.parser.Categorical.MAX_CATEGORICAL_COUNT) {
        Vec.T_ENUM
      } else {
        Vec.T_STR
      }
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
