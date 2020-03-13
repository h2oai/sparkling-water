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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.extensions.serde.ChunkAutoBufferReader
import ai.h2o.sparkling.frame.H2OChunk
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.h2o.utils.{NodeDesc, SupportedTypes}
import org.apache.spark.unsafe.types.UTF8String


private[backend] class Reader(keyName: String,
                              chunkIdx: Int,
                              numRows: Int,
                              nodeDesc: NodeDesc,
                              expectedTypes: Array[Byte],
                              selectedColumnIndices: Array[Int],
                              conf: H2OConf) {
  /** Current row index */
  private var rowIdx: Int = 0

  private val reader = new ChunkAutoBufferReader(
    H2OChunk.getChunkAsInputStream(nodeDesc, conf, keyName, numRows, chunkIdx, expectedTypes, selectedColumnIndices))

  private def returnOption[T](read: ChunkAutoBufferReader => T)(columnNum: Int): Option[T] = {
    Option(read(reader)).filter(_ => !reader.isLastNA)
  }

  private def returnSimple[T](ifMissing: String => T, read: ChunkAutoBufferReader => T)(columnNum: Int): T = {
    val value = read(reader)
    if (reader.isLastNA) ifMissing(s"Row $rowIdx column $columnNum") else value
  }

  protected def booleanAt(source: ChunkAutoBufferReader): Boolean = source.readBoolean()

  protected def byteAt(source: ChunkAutoBufferReader): Byte = source.readByte()

  protected def shortAt(source: ChunkAutoBufferReader): Short = source.readShort()

  protected def intAt(source: ChunkAutoBufferReader): Int = source.readInt()

  protected def longAt(source: ChunkAutoBufferReader): Long = source.readLong()

  protected def floatAt(source: ChunkAutoBufferReader): Float = source.readFloat()

  protected def doubleAt(source: ChunkAutoBufferReader): Double = source.readDouble()

  protected def string(source: ChunkAutoBufferReader) = source.readString()

  protected def utfString(source: ChunkAutoBufferReader) = UTF8String.fromString(string(source))

  protected def timestamp(source: ChunkAutoBufferReader): Long = longAt(source) * 1000

  def hasNext: Boolean = {
    val isNext = rowIdx < numRows
    if (!isNext) {
      reader.close()
    }
    isNext
  }

  def increaseRowIdx(): Unit = rowIdx += 1

  /**
   * This map registers for each type corresponding extractor
   *
   * Given a a column number, returns an Option[T]
   * with the value parsed according to the type.
   * You can override it.
   *
   * A map from type name to option reader
   */
  private lazy val ExtractorsTable: Map[SimpleType[_], ChunkAutoBufferReader => _] = Map(
    Boolean -> booleanAt _,
    Byte -> byteAt _,
    Short -> shortAt _,
    Integer -> intAt _,
    Long -> longAt _,
    Float -> floatAt _,
    Double -> doubleAt _,
    String -> string _,
    UTF8 -> utfString _,
    Timestamp -> timestamp _,
    Date -> timestamp _
  )

  private type OptionReader = Int => Option[Any]

  private type Reader = Int => Any

  private lazy val OptionReadersMap: Map[OptionalType[_], OptionReader] = {
    ExtractorsTable.map {
      case (t, reader) => SupportedTypes.byBaseType(t) -> returnOption(reader) _
    }.toMap
  }

  private lazy val SimpleReadersMap: Map[SimpleType[_], Reader] = {
    ExtractorsTable.map {
      case (t, reader) => t -> returnSimple(t.ifMissing, reader) _
    }.toMap
  }

  lazy val OptionReaders: Map[OptionalType[_], OptionReader] = OptionReadersMap.withDefault(
    t => throw new scala.IllegalArgumentException(s"Type $t conversion is not supported in Sparkling Water"))

  lazy val SimpleReaders: Map[SimpleType[_], Reader] = SimpleReadersMap.withDefault(
    t => throw new scala.IllegalArgumentException(s"Type $t conversion is not supported in Sparkling Water"))
}
