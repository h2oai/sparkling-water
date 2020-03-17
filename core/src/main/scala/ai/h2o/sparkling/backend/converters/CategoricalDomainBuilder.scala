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
class CategoricalDomainBuilder() {

  private val domains = mutable.ArrayBuffer[mutable.LinkedHashMap[String, Int]]()

  private val indexMapping = mutable.OpenHashMap[Int, Int]()

  def stringToIndex(value: String, columnIndex: Int): Int = {
    val domainIndex = indexMapping.getOrElseUpdate(columnIndex, {
      val result = domains.size
      domains.append(mutable.LinkedHashMap[String, Int]())
      result
    })
    val domain = domains(domainIndex)
    domain.getOrElseUpdate(value, domain.size)
  }

  def getDomains(): Array[Array[String]] = domains.map(_.keys.toArray).toArray
}
