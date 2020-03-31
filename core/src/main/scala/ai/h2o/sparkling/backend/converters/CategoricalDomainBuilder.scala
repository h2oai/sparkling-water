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

import scala.collection.mutable

/**
  * This class is not thread safe.
  */
private[backend] class CategoricalDomainBuilder() {

  private val domains = mutable.ArrayBuffer[mutable.LinkedHashMap[String, Int]]()

  private val indexMapping = mutable.OpenHashMap[Int, Int]()

  /**
    * The method adds string value to a corresponding categorical domain and returns position of the value within
    * the domain. If the value is already there, returns just the original position and doesn't make any update.
    * @param value String value
    * @param columnIndex Index of a column determining the categorical domain.
    *                    Indexing also includes columns of other types.
    * @return Index of the value within the categorical domain.
    */
  def addStringToDomain(value: String, columnIndex: Int): Int = {
    val domain = getOrCreateDomain(columnIndex)
    domain.getOrElseUpdate(value, domain.size)
  }

  /**
    * The method does not any category to a domain, just creates a mapping from a columnId to a domain if does not exist
    * @param columnIndex Index of a column determining the categorical domain.
    *                    Indexing also includes columns of other types.
    */
  def markNA(columnIndex: Int): Unit = getOrCreateDomain(columnIndex)

  private def getOrCreateDomain(columnIndex: Int): mutable.LinkedHashMap[String, Int] = {
    val domainIndex = indexMapping.getOrElseUpdate(columnIndex, {
      val result = domains.size
      domains.append(mutable.LinkedHashMap[String, Int]())
      result
    })
    domains(domainIndex)
  }

  def getDomains(): Array[Array[String]] = domains.map(_.keys.toArray).toArray
}
