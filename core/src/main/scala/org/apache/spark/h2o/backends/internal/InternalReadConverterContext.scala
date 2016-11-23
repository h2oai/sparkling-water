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

package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.converters.ReadConverterContext
import org.apache.spark.h2o.utils.ReflectionUtils.NameOfType
import org.apache.spark.h2o.utils.SupportedTypes
import org.apache.spark.unsafe.types.UTF8String
import water.fvec.{Vec, Chunk, Frame}
import water.parser.BufferedString
import water.{DKV, Key}

import scala.language.postfixOps

class InternalReadConverterContext(override val keyName: String, override val chunkIdx: Int) extends ReadConverterContext{
  /** Lazily fetched H2OFrame from K/V store */
  private lazy val fr: Frame = underlyingFrame

  /** Chunks for this partition */
  private lazy val chks: Array[Chunk] = water.fvec.FrameUtils.getChunks(fr, chunkIdx)

  /** Number of rows in this partition */
  lazy val numRows = chks(0)._len
  
  private def returnOption[T](read: Chunk => T)(columnNum: Int): Option[T] = {
    for {
      chunk <- Option(chks(columnNum)) if !chunk.isNA(rowIdx)
      data <- Option(read(chunk))
    } yield data
  }

  private def returnSimple[T](ifMissing: String => T, read: Chunk => T)(columnNum: Int): T = {
    val chunk = chks(columnNum)
      if (chunk.isNA(rowIdx)) ifMissing(s"Row $rowIdx column $columnNum") else read(chunk)
  }

  private def longAt(chunk: Chunk) = chunk.at8(rowIdx)
  private def doubleAt(chunk: Chunk) = chunk.atd(rowIdx)
  private def booleanAt(chunk: Chunk) = longAt(chunk) == 1
  private def byteAt(chunk: Chunk) = longAt(chunk).toByte
  private def intAt(chunk: Chunk) = longAt(chunk).toInt
  private def shortAt(chunk: Chunk) = longAt(chunk).toShort
  private def floatAt(chunk: Chunk) = longAt(chunk).toFloat

  private def categoricalString(chunk: Chunk) = chunk.vec().domain()(longAt(chunk).toInt)

  private def uuidString(chunk: Chunk) = new java.util.UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx)).toString

  // TODO(vlad): take care of this bad typing
  private def timestamp(chunk: Chunk) = longAt(chunk) * 1000L

  private def plainString(chunk: Chunk) = chunk.atStr(new BufferedString(), rowIdx).toString

  private val StringProviders = Map[Byte, (Chunk => String)](
    Vec.T_CAT -> categoricalString,
    Vec.T_UUID -> uuidString,
    Vec.T_STR -> plainString
  ) withDefault((t: Byte) => {
    assert(assertion = false, s"Should never be here, type is $t")
    (_: Chunk) => null
  }
    )

  private def string(chunk: Chunk): String = StringProviders(chunk.vec().get_type())(chunk)

  // TODO(vlad): check if instead of stringification, we could use bytes
  private def utfString(chunk: Chunk) = UTF8String.fromString(string(chunk))

  private def underlyingFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]

  import SupportedTypes._
  /**
    * Given a a column number, returns an Option[T]
    * with the value parsed according to the type.
    * You can override it.
    *
    * A map from type name to option reader
    */
  private lazy val ExtractorsTable: Map[SimpleType[_], Chunk => _] = Map(
    Boolean    -> booleanAt _,
    Byte       -> byteAt _,
    Double     -> doubleAt _,
    Float      -> floatAt _,
    Integer    -> intAt _,
    Long       -> longAt _,
    Short      -> shortAt _,
    String     -> string _,
    UTF8       -> utfString _,
    Timestamp  -> timestamp _
  )

  private lazy val OptionReadersMap: Map[OptionalType[_], OptionReader] =
    ExtractorsTable map {
      case (t, reader) => SupportedTypes.byBaseType(t) -> returnOption(reader) _
    } toMap

  private lazy val OptionReaders: Map[OptionalType[_], OptionReader] = OptionReadersMap withDefault
    (t => throw new scala.IllegalArgumentException(s"Type $t conversion is not supported in Sparkling Water"))

  private lazy val SimpleReadersMap: Map[SimpleType[_], Reader] =
    ExtractorsTable map {
      case (t, reader) => t -> returnSimple(t.ifMissing, reader) _
    } toMap

  private lazy val SimpleReaders: Map[SimpleType[_], Reader] = SimpleReadersMap withDefault
    (t => throw new scala.IllegalArgumentException(s"Type $t conversion is not supported in Sparkling Water"))

  lazy val readerMapByName: Map[NameOfType, Reader] = (OptionReaders ++ SimpleReaders) map {
    case (supportedType, reader) => supportedType.name -> reader
  } toMap

  def columnValueProviders(columnIndexesWithTypes: Array[(Int, SimpleType[_])]): Array[() => Option[Any]] = {
    for {
      (columnIndex, supportedType) <- columnIndexesWithTypes
      reader = OptionReaders(byBaseType(supportedType))
      provider = () => reader.apply(columnIndex)
    } yield provider
  }
}
