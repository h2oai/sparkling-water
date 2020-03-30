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

package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, UserDefinedType}

object ExposeUtils {
  def classForName(className: String): Class[_] = {
    org.apache.spark.util.Utils.classForName(className)
  }

  def isMLVectorUDT(dataType: DataType): Boolean = {
    dataType match {
      case _: ml.linalg.VectorUDT => true
      case _ => false
    }
  }

  def isAnyVectorUDT(dataType: DataType): Boolean = {
    dataType match {
      case _: ml.linalg.VectorUDT => true
      case _: mllib.linalg.VectorUDT => true
      case _ => false
    }
  }

  def isUDT(dataType: DataType): Boolean = {
    dataType match {
      case _: UserDefinedType[_] => true
      case _ => false
    }
  }

  def hiveClassesArePresent: Boolean = {
    SparkSession.hiveClassesArePresent
  }
}
