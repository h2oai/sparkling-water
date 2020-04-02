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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.extensions.serde.ChunkSerdeConstants

import scala.collection.mutable

/**
  * This class is not thread safe.
  */
private[backend] class CategoricalDomainBuilder(expectedTypes: Array[Byte]) {

  private def categoricalIndexes: Array[Int] =
    for ((eType, index) <- expectedTypes.zipWithIndex if eType == ChunkSerdeConstants.EXPECTED_CATEGORICAL) yield index

  private val indexMapping = categoricalIndexes.zipWithIndex.toMap

  private val domains = (0 until indexMapping.size).map(_ => mutable.LinkedHashMap[String, Int]()).toArray

  /**
    * The method adds string value to a corresponding categorical domain and returns position of the value within
    * the domain. If the value is already there, returns just the original position and doesn't make any update.
    * @param value String value
    * @param columnIndex Index of a column determining the categorical domain.
    *                    Indexing also includes columns of other types.
    * @return Index of the value within the categorical domain.
    */
  def addStringToDomain(value: String, columnIndex: Int): Int = {
    val domainIndex = indexMapping(columnIndex)
    val domain = domains(domainIndex)
    domain.getOrElseUpdate(value, domain.size)
  }

  def getDomains(): Array[Array[String]] = domains.map(_.keys.toArray)
}
