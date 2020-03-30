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

package ai.h2o.sparkling.utils

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Base64

object Base64Encoding {
  val byteOrder = ByteOrder.LITTLE_ENDIAN

  def encode(data: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(data)
  }

  def encode(data: Array[Int]): String = {
    val buffer = ByteBuffer.allocate(data.length * 4).order(byteOrder)
    data.foreach(int => buffer.putInt(int))
    Base64.getEncoder.encodeToString(buffer.array())
  }

  def encode(data: Array[Long]): String = {
    val buffer = ByteBuffer.allocate(data.length * 8).order(byteOrder)
    data.foreach(long => buffer.putLong(long))
    Base64.getEncoder.encodeToString(buffer.array())
  }

  def decode(string: String): Array[Byte] = {
    Base64.getDecoder.decode(string)
  }

  def decodeToIntArray(string: String): Array[Int] = {
    val bytes = decode(string)
    val buffer = ByteBuffer.wrap(bytes).order(byteOrder).asIntBuffer
    val result = new Array[Int](buffer.remaining)
    buffer.get(result)
    result
  }

  def decodeToLongArray(string: String): Array[Long] = {
    val bytes = decode(string)
    val buffer = ByteBuffer.wrap(bytes).order(byteOrder).asLongBuffer
    val result = new Array[Long](buffer.remaining)
    buffer.get(result)
    result
  }
}
