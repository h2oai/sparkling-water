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

package ai.h2o.sparkling.extensions.serde

trait ChunkSerdeConstants {
  val EXPECTED_BOOL = 0
  val EXPECTED_BYTE = 1
  val EXPECTED_CHAR = 2
  val EXPECTED_SHORT = 3
  val EXPECTED_INT = 4
  val EXPECTED_FLOAT = 5
  val EXPECTED_LONG = 6
  val EXPECTED_DOUBLE = 7
  val EXPECTED_STRING = 8
  val EXPECTED_TIMESTAMP = 9
  val EXPECTED_VECTOR = 10

  /**
    * Meta Information used to specify whether we should expect sparse or dense vector
    */
  val VECTOR_IS_SPARSE = true
  val VECTOR_IS_DENSE = false

  /**
    * This is used to inform us that another byte is coming.
    * That byte can be either {@code MARKER_ORIGINAL_VALUE} or {@code MARKER_NA}. If it's
    * {@code MARKER_ORIGINAL_VALUE}, that means
    * the value sent is in the previous data sent, otherwise the value is NA.
    */
  val NUM_MARKER_NEXT_BYTE_FOLLOWS: Byte = 127

  /**
    * Same as above, but for Strings. We are using unicode code for CONTROL, which should be very very rare
    * String to send as usual String data.
    */
  val STR_MARKER_NEXT_BYTE_FOLLOWS = "\u0080"

  /**
    * Marker informing us that the data are not NA and are stored in the previous byte
    */
  val MARKER_ORIGINAL_VALUE = 0

  /**
    * Marker informing us that the data being sent is NA
    */
  val MARKER_NA = 1
}

object ChunkSerdeConstants extends ChunkSerdeConstants
