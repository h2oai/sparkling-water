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

package ai.h2o.sparkling.ml.utils

import org.apache.spark.sql.types._

object DatasetShape extends Enumeration {
  type DatasetShape = Value
  val Flat, StructsOnly, Nested = Value

  def getDatasetShape(schema: StructType): DatasetShape = {
    def mergeShape(first: DatasetShape, second: DatasetShape): DatasetShape = (first, second) match {
      case (DatasetShape.Nested, _) => DatasetShape.Nested
      case (_, DatasetShape.Nested) => DatasetShape.Nested
      case (DatasetShape.StructsOnly, _) => DatasetShape.StructsOnly
      case (_, DatasetShape.StructsOnly) => DatasetShape.StructsOnly
      case _ => DatasetShape.Flat
    }

    def fieldToShape(field: StructField): DatasetShape = field.dataType match {
      case _: ArrayType | _: MapType | _: BinaryType => DatasetShape.Nested
      case s: StructType => mergeShape(DatasetShape.StructsOnly, getDatasetShape(s))
      case _ => DatasetShape.Flat
    }

    schema.fields.foldLeft(DatasetShape.Flat) { (acc, field) =>
      val fieldShape = fieldToShape(field)
      mergeShape(acc, fieldShape)
    }
  }
}
