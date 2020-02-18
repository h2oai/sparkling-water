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

package ai.h2o.sparkling.backend.shared

import org.apache.spark.h2o.utils.SupportedTypes.{SimpleType, SupportedType, bySparkType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

/**
 * The abstract class contains common methods for client-based and REST-based DataFrames
 */
private[backend] trait H2ODataFrameBase extends H2OSparkEntity {

  protected def types: Array[DataType]

  protected def indexToSupportedType(index: Int): SupportedType

  /** Selected column indices */
  val selectedColumnIndices: Array[Int]


  abstract class H2ODataFrameIterator extends H2OChunkIterator[InternalRow] {

    private lazy val columnIndicesWithTypes: Array[(Int, SimpleType[_])] = selectedColumnIndices map (i => (i, bySparkType(types(i))))

    /*a sequence of value providers, per column*/
    private lazy val columnValueProviders: Array[() => Option[Any]] = reader.columnValueProviders(columnIndicesWithTypes)

    def readOptionalData: Seq[Option[Any]] = columnValueProviders map (_ ())

    private def readRow: InternalRow = {
      val optionalData: Seq[Option[Any]] = readOptionalData
      val nullableData: Seq[Any] = optionalData map (_ orNull)
      InternalRow.fromSeq(nullableData)
    }

    override def next(): InternalRow = {
      val row = readRow
      reader.increaseRowIdx()
      row
    }
  }

}
