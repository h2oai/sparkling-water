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

package org.apache.spark.h2o.converters

import org.apache.spark.h2o.utils.ReflectionUtils._
import org.apache.spark.h2o.utils.SupportedTypes.SimpleType

import scala.language.postfixOps

/**
  * Methods which each ReadConverterContext has to implement.
  *
  * Read Converter Context is a class which holds the state of connection/chunks and allows us to read/download data from those chunks
  * via unified API
  */
trait ReadConverterContext {

  // TODO(vlad): figure out if this is needed
  /** Key pointing to underlying H2OFrame */
  val keyName: String

  // TODO(vlad): figure out if this is needed
  /** Chunk Idx/Partition index */
  val chunkIdx: Int

  /** Current row index */
  var rowIdx: Int = 0

  def numRows: Int
  def increaseRowIdx() = rowIdx += 1

  def hasNext = rowIdx < numRows


  type OptionReader = Int => Option[Any]

  type Reader = Int => Any

  def readerMapByName: Map[NameOfType, Reader]

  /**
    * For a given array of source column indexes and required data types,
    * produces an array of value providers.
    *
    * @param columnIndexesWithTypes lists which columns we need, and what are the required types
    * @return an array of value providers. Each provider gives the current column value
    */
  def columnValueProviders(columnIndexesWithTypes: Array[(Int, SimpleType[_])]): Array[() => Option[Any]]
}
