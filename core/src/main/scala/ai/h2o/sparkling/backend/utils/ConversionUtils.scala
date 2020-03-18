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

package ai.h2o.sparkling.backend.utils

import ai.h2o.sparkling.extensions.serde.ChunkSerdeConstants

private[backend] object ConversionUtils {

  private[backend] def expectedTypesFromClasses(classes: Array[Class[_]]): Array[Byte] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ChunkSerdeConstants.EXPECTED_BOOL
      } else if (clazz == classOf[java.lang.Byte]) {
        ChunkSerdeConstants.EXPECTED_BYTE
      } else if (clazz == classOf[java.lang.Short]) {
        ChunkSerdeConstants.EXPECTED_SHORT
      } else if (clazz == classOf[java.lang.Character]) {
        ChunkSerdeConstants.EXPECTED_CHAR
      } else if (clazz == classOf[java.lang.Integer]) {
        ChunkSerdeConstants.EXPECTED_INT
      } else if (clazz == classOf[java.lang.Long]) {
        ChunkSerdeConstants.EXPECTED_LONG
      } else if (clazz == classOf[java.lang.Float]) {
        ChunkSerdeConstants.EXPECTED_FLOAT
      } else if (clazz == classOf[java.lang.Double]) {
        ChunkSerdeConstants.EXPECTED_DOUBLE
      } else if (clazz == classOf[java.lang.String]) {
        ChunkSerdeConstants.EXPECTED_STRING
      } else if (clazz == classOf[java.sql.Timestamp] || clazz == classOf[java.sql.Date]) {
        ChunkSerdeConstants.EXPECTED_TIMESTAMP
      } else if (clazz == classOf[org.apache.spark.ml.linalg.Vector]) {
        ChunkSerdeConstants.EXPECTED_VECTOR
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }
}
