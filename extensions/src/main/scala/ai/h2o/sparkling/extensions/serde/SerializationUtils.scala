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

import java.sql.Timestamp

import water.AutoBuffer

object SerializationUtils {
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
  private[serde] val NUM_MARKER_NEXT_BYTE_FOLLOWS: Byte = 127

  /**
    * Same as above, but for Strings. We are using unicode code for CONTROL, which should be very very rare
    * String to send as usual String data.
    */
  private[serde] val STR_MARKER_NEXT_BYTE_FOLLOWS = "\u0080"

  /**
    * Marker informing us that the data are not NA and are stored in the previous byte
    */
  private[serde] val MARKER_ORIGINAL_VALUE = 0

  /**
    * Marker informing us that the data being sent is NA
    */
  private[serde] val MARKER_NA = 1

  def sendIntArray(ab: AutoBuffer, data: Array[Int]): Unit = {
    ab.putA4(data)
  }

  def sendDoubleArray(ab: AutoBuffer, data: Array[Double]): Unit = {
    ab.putA8d(data)
  }

  def sendBoolean(ab: AutoBuffer, data: Boolean): Unit = {
    sendBoolean(ab, if (data) 1.toByte
    else 0.toByte)
  }

  def sendBoolean(ab: AutoBuffer, boolData: Byte): Unit = {
    ab.put1(boolData)
    putMarker(ab, boolData)
  }

  def sendByte(ab: AutoBuffer, data: Byte): Unit = {
    ab.put1(data)
    putMarker(ab, data)
  }

  def sendChar(ab: AutoBuffer, data: Char): Unit = {
    ab.put2(data)
    putMarker(ab, data)
  }

  def sendShort(ab: AutoBuffer, data: Short): Unit = {
    ab.put2s(data)
    putMarker(ab, data)
  }

  def sendInt(ab: AutoBuffer, data: Int): Unit = {
    ab.putInt(data)
    putMarker(ab, data)
  }

  def sendLong(ab: AutoBuffer, data: Long): Unit = {
    ab.put8(data)
    putMarker(ab, data)
  }

  def sendFloat(ab: AutoBuffer, data: Float): Unit = {
    ab.put4f(data)
  }

  def sendDouble(ab: AutoBuffer, data: Double): Unit = {
    ab.put8d(data)
  }

  def sendString(ab: AutoBuffer, data: String): Unit = {
    ab.putStr(data)
    if (data != null && data == STR_MARKER_NEXT_BYTE_FOLLOWS) ab.put1(MARKER_ORIGINAL_VALUE)
  }

  def sendTimestamp(ab: AutoBuffer, time: Long): Unit = {
    sendLong(ab, time)
  }

  def sendTimestamp(ab: AutoBuffer, data: Timestamp): Unit = {
    sendLong(ab, data.getTime)
  }

  private[serde] def sendNA(ab: AutoBuffer, expectedType: Byte): Unit = expectedType match {
    case EXPECTED_BOOL | EXPECTED_BYTE =>
      ab.put1(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case EXPECTED_CHAR =>
      ab.put2(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case EXPECTED_SHORT =>
      ab.put2s(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case EXPECTED_INT =>
      ab.putInt(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case EXPECTED_TIMESTAMP | EXPECTED_LONG =>
      ab.put8(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case EXPECTED_FLOAT =>
      ab.put4f(Float.NaN)
    case EXPECTED_DOUBLE =>
      ab.put8d(Double.NaN)
    case EXPECTED_STRING =>
      ab.putStr(STR_MARKER_NEXT_BYTE_FOLLOWS)
      ab.put1(MARKER_NA)
    case _ =>
      throw new IllegalArgumentException("Unknown expected type " + expectedType)
  }

  /**
    * Sends another byte as a marker if it's needed and send the data
    */
  private def putMarker(ab: AutoBuffer, data: Long): Unit = {
    if (data == NUM_MARKER_NEXT_BYTE_FOLLOWS) { // we need to send another byte because zero is represented as 00 ( 2 bytes )
      ab.put1(MARKER_ORIGINAL_VALUE)
    }
  }

  def isNA(ab: AutoBuffer, data: Boolean): Boolean = isNA(ab, if (data) 1.toLong else 0)

  def isNA(ab: AutoBuffer, data: Long): Boolean = data == NUM_MARKER_NEXT_BYTE_FOLLOWS && ab.get1 == MARKER_NA

  def isNA(data: Double): Boolean = data == Double.NaN

  def isNA(ab: AutoBuffer, data: Timestamp): Boolean = isNA(ab, data.getTime)

  def isNA(ab: AutoBuffer, data: String): Boolean = data != null && data == STR_MARKER_NEXT_BYTE_FOLLOWS && ab.get1 == MARKER_NA
}
