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

package ai.h2o.sparkling.frame

import org.apache.spark.h2o.utils.NodeDesc

/* This case class contains metadata describing H2O Frame accessed via REST API */
case class H2OFrame(
    frameId: String,
    columns: Array[H2OColumn],
    chunks: Array[H2OChunk]) {
  lazy val numberOfRows: Long = chunks.foldLeft(0L)((acc, chunk) => acc + chunk.numberOfRows)

  def numberOfColumns: Int = columns.length
}

case class H2OColumn(
    name: String,
    dataType: H2OColumnType.Value,
    min: Double,
    max: Double,
    mean: Double,
    sigma: Double,
    numberOfZeros: Long,
    numberOfMissingElements: Long,
    percentiles: Array[Double],
    domain: Array[String],
    domainCardinality: Long) {
  def nullable: Boolean = numberOfMissingElements > 0
}

object H2OColumnType extends Enumeration {
  val enum, string, int, real, time, uuid = Value

  def fromString(dataType: String): Value = {
    dataType match {
      case "enum" => `enum`
      case "string" => string
      case "int" => int
      case "real" => real
      case "time" => time
      case "uuid" => uuid
      case unknown => throw new RuntimeException(s"Unknown H2O's Data type $unknown")
    }
  }
}


case class H2OChunk(index: Int, numberOfRows: Long, location: NodeDesc)
